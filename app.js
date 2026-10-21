import {
    ABS_BRAKE_SWITCH,
    ABS_BRAKING_STATE,
    ST_ABS,
    ST_DSC,
    TimeoutError,
    buildGetConfig,
    buildGetStatus,
    buildIdentify,
    decodeResponse,
    label,
} from './protocol.js';

const BAUD = 115200;
const MAX_LOG_ROWS = 200000;
const CHART_WINDOW_MS = 30000;
const LIVE_HISTORY_MAX = 4000;
const STATUS_TIMEOUT_MS = 1000;
const MISSES_BEFORE_DROP = 3;

const SPEED_SERIES = [
    { key: 'vehicle', name: 'Vehicle', color: '#f4f4f5' },
    { key: 'lf', name: 'LF', color: '#4da3ff' },
    { key: 'rf', name: 'RF', color: '#ffb020' },
    { key: 'lr', name: 'LR', color: '#3ddc84' },
    { key: 'rr', name: 'RR', color: '#ff5d73' },
];

const PRESS_SERIES = [
    { key: 'input', name: 'Input', color: '#c084fc' },
    { key: 'lf', name: 'LF', color: '#4da3ff' },
    { key: 'rf', name: 'RF', color: '#ffb020' },
    { key: 'lr', name: 'LR', color: '#3ddc84' },
    { key: 'rr', name: 'RR', color: '#ff5d73' },
];

const $ = (id) => document.getElementById(id);

const ui = {
    banner: $('banner'),
    connect: $('connect'),
    disconnect: $('disconnect'),
    record: $('record'),
    clear: $('clear'),
    exportCsv: $('export'),
    interval: $('interval'),
    brakeOnly: $('brake-only'),
    windowSel: $('window'),
    state: $('state'),
    error: $('error'),
    meta: $('meta'),
    dot: $('dot'),
    corners: ['lf', 'rf', 'lr', 'rr'].map((name) => ({
        name,
        speed: $(`speed-${name}`),
        press: $(`press-${name}`),
    })),
    vehicle: $('speed-vehicle'),
    input: $('press-input'),
    brake: $('brake-switch'),
    absSwitch: $('abs-switch'),
    braking: $('braking'),
    dsc: $('st-dsc'),
    abs: $('st-abs'),
    fault: $('fault'),
    temp: $('temp'),
    canRx: $('can-rx'),
    canTxOk: $('can-tx-ok'),
    canTxErr: $('can-tx-err'),
    logCount: $('log-count'),
    logTime: $('log-time'),
    rate: $('rate'),
    missed: $('missed'),
    body: $('samples'),
    speedCanvas: $('speed-chart'),
    pressCanvas: $('press-chart'),
    speedLegend: $('speed-legend'),
    pressLegend: $('press-legend'),
};

const state = {
    port: null,
    session: null,
    generation: 0,
    cmdId: 100,
    chain: Promise.resolve(),
    ident: null,
    config: null,
    recording: false,
    log: [],
    history: [],
    recent: [],
    recordStarted: 0,
    misses: 0,
    hiddenSpeed: new Set(),
    hiddenPress: new Set(),
};

function setError(message) {
    ui.error.textContent = message || '';
}

function setState(text, mode) {
    ui.state.textContent = text;
    ui.dot.className = 'dot' + (mode ? ` ${mode}` : '');
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

class SerialSession {
    constructor(port) {
        this.port = port;
        this.reader = port.readable.getReader();
        this.writer = port.writable.getWriter();
        this.buf = new Uint8Array(0);
        this.waiters = [];
        this.failed = null;
        this.pumping = this.pump();
    }

    append(chunk) {
        if (!chunk?.length) return;
        const next = new Uint8Array(this.buf.length + chunk.length);
        next.set(this.buf, 0);
        next.set(chunk, this.buf.length);
        this.buf = next;
    }

    dispatch() {
        while (this.waiters.length && this.buf.length >= this.waiters[0].n) {
            const waiter = this.waiters.shift();
            const out = this.buf.slice(0, waiter.n);
            this.buf = this.buf.slice(waiter.n);
            clearTimeout(waiter.timer);
            waiter.resolve(out);
        }
    }

    read(n, timeoutMs) {
        if (this.failed) return Promise.reject(this.failed);
        return new Promise((resolve, reject) => {
            const waiter = { n, resolve, reject, timer: 0 };
            waiter.timer = setTimeout(() => {
                const index = this.waiters.indexOf(waiter);
                if (index >= 0) this.waiters.splice(index, 1);
                reject(new TimeoutError(`No serial data for ${timeoutMs} ms`));
            }, timeoutMs);
            this.waiters.push(waiter);
            this.dispatch();
        });
    }

    discard() {
        this.buf = new Uint8Array(0);
    }

    fail(error) {
        if (this.failed) return;
        this.failed = error;
        for (const waiter of this.waiters) {
            clearTimeout(waiter.timer);
            waiter.reject(error);
        }
        this.waiters = [];
    }

    async pump() {
        try {
            while (true) {
                const { value, done } = await this.reader.read();
                if (done) {
                    this.fail(new Error('Serial stream closed'));
                    break;
                }
                this.append(value);
                this.dispatch();
            }
        } catch (error) {
            this.fail(error);
        }
    }

    async write(bytes) {
        if (this.failed) throw this.failed;
        await this.writer.write(bytes);
    }

    async close() {
        this.fail(new Error('Closed'));
        try { await this.reader.cancel(); } catch { /* already gone */ }
        try { this.reader.releaseLock(); } catch { /* already released */ }
        try { this.writer.releaseLock(); } catch { /* already released */ }
    }
}

function enqueue(task) {
    const run = state.chain.then(task, task);
    state.chain = run.then(() => {}, () => {});
    return run;
}

async function readFrame(session, timeoutMs) {
    const lengthByte = await session.read(1, timeoutMs);
    const payload = await session.read(lengthByte[0] + 1, timeoutMs);
    return decodeResponse(payload);
}

async function transact(session, frame, timeoutMs) {
    return enqueue(async () => {
        await session.write(frame);
        const response = await readFrame(session, timeoutMs);
        return response;
    });
}

async function transactChecked(session, buildFrame, timeoutMs) {
    const cmdId = state.cmdId++;
    let response = await transact(session, buildFrame(cmdId), timeoutMs);
    if (response.cmdId !== cmdId) {
        session.discard();
        await sleep(100);
        session.discard();
        const retryId = state.cmdId++;
        response = await transact(session, buildFrame(retryId), timeoutMs);
        if (response.cmdId !== retryId) {
            throw new Error(`Command id mismatch, expected ${retryId} and received ${response.cmdId}`);
        }
    }
    if (response.nak !== undefined) {
        throw new Error(response.nak ? `Device rejected the command: ${response.nak}` : 'Device rejected the command');
    }
    return response;
}

function sampleFromStatus(status, rttMs) {
    return {
        t: Date.now(),
        rtt: Math.round(rttMs),
        vehicle: status.vehicle,
        vehicleInvalid: status.vehicleInvalid,
        wss: status.wss,
        wssInvalid: status.wssInvalid,
        pressInput: status.pressInput,
        press: status.press,
        brakeSwitch: status.brakeSwitch,
        absBrakeSwitch: status.absBrakeSwitch,
        absBrakingState: status.absBrakingState,
        faultLight: status.faultLight,
        stAbs: status.stAbs,
        stDsc: status.stDsc,
        canRx: status.canRx,
        canTxOk: status.canTxOk,
        canTxErr: status.canTxErr,
        tempC: status.tempC,
    };
}

function renderValue(node, value, invalid) {
    if (invalid) {
        node.textContent = 'invalid';
        node.classList.add('invalid');
    } else {
        node.textContent = String(value);
        node.classList.remove('invalid');
    }
}

function renderSample(sample) {
    renderValue(ui.vehicle, sample.vehicle, sample.vehicleInvalid);
    ui.input.textContent = String(sample.pressInput);
    ui.corners.forEach((corner, index) => {
        renderValue(corner.speed, sample.wss[index], sample.wssInvalid[index]);
        corner.press.textContent = String(sample.press[index]);
    });
    ui.brake.textContent = sample.brakeSwitch ? 'pressed' : 'released';
    ui.absSwitch.textContent = label(ABS_BRAKE_SWITCH, sample.absBrakeSwitch);
    ui.braking.textContent = label(ABS_BRAKING_STATE, sample.absBrakingState);
    ui.dsc.textContent = label(ST_DSC, sample.stDsc);
    ui.abs.textContent = label(ST_ABS, sample.stAbs);
    ui.fault.textContent = sample.faultLight ? 'on' : 'off';
    ui.temp.textContent = String(sample.tempC);
    ui.canRx.textContent = String(sample.canRx);
    ui.canTxOk.textContent = String(sample.canTxOk);
    ui.canTxErr.textContent = String(sample.canTxErr);
}

function pushHistory(sample) {
    state.history.push(sample);
    if (state.history.length > LIVE_HISTORY_MAX) {
        state.history.splice(0, state.history.length - LIVE_HISTORY_MAX);
    }
    state.recent.push(sample);
    if (state.recent.length > 8) state.recent.shift();
}

function clock(ms) {
    const date = new Date(ms);
    const p = (n, w = 2) => String(n).padStart(w, '0');
    return `${p(date.getHours())}:${p(date.getMinutes())}:${p(date.getSeconds())}.${p(date.getMilliseconds(), 3)}`;
}

function renderTable() {
    ui.body.replaceChildren();
    for (const sample of state.recent) {
        const row = document.createElement('tr');
        const cells = [
            clock(sample.t),
            sample.vehicleInvalid ? '' : sample.vehicle,
            ...sample.wss.map((value, index) => (sample.wssInvalid[index] ? '' : value)),
            sample.pressInput,
            ...sample.press,
        ];
        for (const cell of cells) {
            const td = document.createElement('td');
            td.textContent = String(cell);
            row.append(td);
        }
        ui.body.append(row);
    }
}

function renderStats() {
    ui.logCount.textContent = state.log.length.toLocaleString();
    if (state.recording && state.recordStarted) {
        const seconds = (Date.now() - state.recordStarted) / 1000;
        ui.logTime.textContent = seconds.toFixed(1) + ' s';
    } else {
        ui.logTime.textContent = state.log.length ? 'stopped' : '—';
    }
    const windowSamples = state.history.filter((sample) => Date.now() - sample.t < 2000);
    ui.rate.textContent = windowSamples.length ? (windowSamples.length / 2).toFixed(1) + ' Hz' : '—';
    ui.missed.textContent = String(state.misses);
}

function chartWindowMs() {
    return Number(ui.windowSel.value) * 1000;
}

function seriesValue(sample, chart, key) {
    if (chart === 'speed') {
        if (key === 'vehicle') return sample.vehicleInvalid ? null : sample.vehicle;
        const index = { lf: 0, rf: 1, lr: 2, rr: 3 }[key];
        return sample.wssInvalid[index] ? null : sample.wss[index];
    }
    if (key === 'input') return sample.pressInput;
    return sample.press[{ lf: 0, rf: 1, lr: 2, rr: 3 }[key]];
}

function drawChart(canvas, series, hidden, chart) {
    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth || 300;
    const height = canvas.clientHeight || 220;
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    ctx.fillStyle = '#101315';
    ctx.fillRect(0, 0, width, height);

    const pad = { l: 44, r: 12, t: 12, b: 22 };
    const plotW = width - pad.l - pad.r;
    const plotH = height - pad.t - pad.b;
    const now = state.history.length ? state.history[state.history.length - 1].t : Date.now();
    const windowMs = chartWindowMs();
    const t0 = now - windowMs;
    const points = state.history.filter((sample) => sample.t >= t0);

    let min = 0;
    let max = 1;
    for (const item of series) {
        if (hidden.has(item.key)) continue;
        for (const sample of points) {
            const value = seriesValue(sample, chart, item.key);
            if (value === null || Number.isNaN(value)) continue;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
    }
    if (max === min) max = min + 1;
    const span = max - min;
    min -= span * 0.06;
    max += span * 0.08;

    ctx.strokeStyle = '#2a3136';
    ctx.fillStyle = '#8b958e';
    ctx.font = '11px Consolas, monospace';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = pad.t + (plotH * i) / 4;
        ctx.beginPath();
        ctx.moveTo(pad.l, y);
        ctx.lineTo(width - pad.r, y);
        ctx.stroke();
        const value = max - ((max - min) * i) / 4;
        ctx.fillText(value.toFixed(0), 4, y + 4);
    }
    ctx.fillText(`${windowMs / 1000}s`, width - pad.r - 24, height - 6);

    for (const item of series) {
        if (hidden.has(item.key)) continue;
        ctx.beginPath();
        ctx.strokeStyle = item.color;
        ctx.lineWidth = item.key === 'vehicle' || item.key === 'input' ? 2 : 1.4;
        let started = false;
        let lastX;
        let lastY;
        for (const sample of points) {
            const value = seriesValue(sample, chart, item.key);
            if (value === null) {
                started = false;
                continue;
            }
            const x = pad.l + ((sample.t - t0) / windowMs) * plotW;
            const y = pad.t + ((max - value) / (max - min)) * plotH;
            lastX = x;
            lastY = y;
            if (!started) {
                ctx.moveTo(x, y);
                started = true;
            } else {
                ctx.lineTo(x, y);
            }
        }
        ctx.stroke();
        if (lastX !== undefined) {
            ctx.fillStyle = item.color;
            ctx.beginPath();
            ctx.arc(lastX, lastY, 2.4, 0, Math.PI * 2);
            ctx.fill();
        }
    }
}

function draw() {
    drawChart(ui.speedCanvas, SPEED_SERIES, state.hiddenSpeed, 'speed');
    drawChart(ui.pressCanvas, PRESS_SERIES, state.hiddenPress, 'press');
}

function acceptSample(sample) {
    pushHistory(sample);
    const store = state.recording && !(ui.brakeOnly.checked && !sample.brakeSwitch);
    if (store) {
        if (state.log.length >= MAX_LOG_ROWS) {
            state.recording = false;
            ui.record.setAttribute('aria-pressed', 'false');
            ui.record.textContent = 'Record';
            setError(`Log is full at ${MAX_LOG_ROWS.toLocaleString()} samples. Export the CSV, then clear it.`);
        } else {
            state.log.push(sample);
        }
    }
    renderSample(sample);
    renderTable();
    renderStats();
    draw();
}

function bit(value) {
    return value ? 1 : 0;
}

function csvEscape(value) {
    const text = String(value);
    if (/[",\r\n]/.test(text)) return `"${text.replaceAll('"', '""')}"`;
    return text;
}

function buildCsv() {
    const ident = state.ident;
    const config = state.config;
    const header = [
        '# MK60E5 companion log',
        `# sw=${ident?.version ?? ''} hw=${ident?.hardwareRevision ?? ''} link=${ident ? (ident.bluetooth ? 'bluetooth' : 'usb') : ''} sn=${ident?.serialHex ?? ''}`,
        `# abs=${config?.typeName ?? ''} speedo_pulse_per_km=${config?.speedoPulsePerKm ?? ''}`,
        [
            'timestamp_iso', 'elapsed_ms', 'rtt_ms',
            'vehicle_kmh', 'vehicle_invalid',
            'wss_lf_kmh', 'wss_lf_invalid',
            'wss_rf_kmh', 'wss_rf_invalid',
            'wss_lr_kmh', 'wss_lr_invalid',
            'wss_rr_kmh', 'wss_rr_invalid',
            'press_input_bar', 'press_lf_bar', 'press_rf_bar', 'press_lr_bar', 'press_rr_bar',
            'brake_switch', 'abs_brake_switch', 'abs_braking_state',
            'fault_light', 'st_abs', 'st_dsc',
            'can_rx', 'can_tx_ok', 'can_tx_err', 'temp_c',
        ].join(','),
    ];
    const t0 = state.log[0]?.t ?? 0;
    const rows = state.log.map((sample) => [
        new Date(sample.t).toISOString(),
        sample.t - t0,
        sample.rtt,
        sample.vehicle,
        bit(sample.vehicleInvalid),
        sample.wss[0], bit(sample.wssInvalid[0]),
        sample.wss[1], bit(sample.wssInvalid[1]),
        sample.wss[2], bit(sample.wssInvalid[2]),
        sample.wss[3], bit(sample.wssInvalid[3]),
        sample.pressInput, sample.press[0], sample.press[1], sample.press[2], sample.press[3],
        bit(sample.brakeSwitch),
        sample.absBrakeSwitch,
        sample.absBrakingState,
        bit(sample.faultLight),
        sample.stAbs,
        sample.stDsc,
        sample.canRx,
        sample.canTxOk,
        sample.canTxErr,
        sample.tempC,
    ].map(csvEscape).join(','));
    return header.concat(rows).join('\r\n') + '\r\n';
}

function exportCsv() {
    if (!state.log.length) {
        setError('The log is empty. Press Record, then export.');
        return;
    }
    const blob = new Blob([buildCsv()], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    const stamp = new Date().toISOString().replace(/[:.]/g, '-').replace('T', '_').slice(0, 19);
    link.href = url;
    link.download = `mk60e5-log-${stamp}.csv`;
    link.click();
    URL.revokeObjectURL(url);
    setError('');
}

function intervalMs() {
    const value = Number(ui.interval.value);
    if (!Number.isFinite(value) || value < 0) return 1;
    return Math.min(value, 5000);
}

function lostDevice(error) {
    const message = String(error?.message || error);
    return message.includes('device has been lost') || message.includes('disconnected') || message.includes('Serial stream closed');
}

async function poll(generation, session) {
    while (generation === state.generation) {
        const started = performance.now();
        try {
            const response = await transactChecked(session, buildGetStatus, STATUS_TIMEOUT_MS);
            if (generation !== state.generation) return;
            if (!response.status) {
                throw new Error('Status response did not include wheel or pressure data');
            }
            const recovered = state.misses > 0;
            state.misses = 0;
            if (recovered) setError('');
            acceptSample(sampleFromStatus(response.status, performance.now() - started));
        } catch (error) {
            if (generation !== state.generation) return;
            if (lostDevice(error)) {
                await disconnect(`Bluetooth serial link dropped: ${error.message}`);
                return;
            }
            state.misses += 1;
            renderStats();
            setError(error instanceof TimeoutError ? 'Waiting for the companion…' : error.message);
            if (state.misses >= MISSES_BEFORE_DROP) {
                await disconnect(error.message || 'The companion stopped responding');
                return;
            }
        }
        const pause = intervalMs();
        if (pause) await sleep(pause);
    }
}

function fillMeta() {
    ui.meta.replaceChildren();
    const rows = [];
    if (state.ident) {
        const ident = state.ident;
        rows.push(
            ['Software', String(ident.version)],
            ['Hardware', String(ident.hardwareRevision)],
            ['Link', ident.bluetooth ? 'Bluetooth serial' : 'USB serial'],
            ['Converter', ident.hallVr ? 'Hall/VR' : 'VR only'],
            ['Serial', ident.serialHex || '—'],
        );
    }
    if (state.config) {
        const config = state.config;
        rows.push(
            ['ABS', config.typeName],
            ['Wheel dirs LF RF LR RR', [config.dirLf, config.dirRf, config.dirLr, config.dirRr].map((v) => (v ? 'rev' : 'fwd')).join(' ')],
            ['Speedo', `${config.speedoPulsePerKm} pulse/km`],
        );
    }
    for (const [name, value] of rows) {
        const dt = document.createElement('span');
        dt.textContent = name;
        const dd = document.createElement('span');
        dd.textContent = value;
        ui.meta.append(dt, dd);
    }
}

async function disconnect(reason) {
    state.generation += 1;
    state.recording = false;
    ui.record.setAttribute('aria-pressed', 'false');
    ui.record.textContent = 'Record';
    const session = state.session;
    state.session = null;
    state.port = null;
    if (session) {
        await session.close();
        try { await session.port.close(); } catch { /* port may already be closed */ }
    }
    ui.connect.disabled = false;
    ui.disconnect.disabled = true;
    setState('Disconnected', '');
    if (reason) setError(reason);
}

async function connect() {
    if (!navigator.serial) return;
    setError('');
    let port;
    try {
        port = await navigator.serial.requestPort();
        await port.open({ baudRate: BAUD });
    } catch (error) {
        if (error?.name === 'NotFoundError') return;
        setError(error.message || 'Could not open the serial port');
        return;
    }

    state.generation += 1;
    const generation = state.generation;
    state.port = port;
    state.ident = null;
    state.config = null;
    state.misses = 0;
    state.cmdId = 100;
    state.chain = Promise.resolve();
    ui.connect.disabled = true;
    ui.disconnect.disabled = false;
    setState('Connecting…', 'on');

    const session = new SerialSession(port);
    state.session = session;
    port.addEventListener('disconnect', () => {
        if (state.generation === generation) {
            disconnect('Bluetooth serial device disconnected');
        }
    }, { once: true });

    try {
        const identResponse = await transactChecked(session, buildIdentify, 2000);
        if (!identResponse.ident) throw new Error('Companion did not identify');
        if (identResponse.ident.bootloader) throw new Error('Companion is in the bootloader. Open the config tool and boot the app first.');
        state.ident = identResponse.ident;
        const configResponse = await transactChecked(session, buildGetConfig, 2000);
        state.config = configResponse.config ?? null;
        fillMeta();
        const abs = state.config?.typeName ?? 'unknown ABS';
        setState(`Connected · ${abs}`, 'on');
        if (state.config && state.config.type !== 2) {
            setError(`${abs} is selected on the companion. Wheel-speed channels are still logged. Line pressures are the MK60E5 fields.`);
        }
        poll(generation, session);
    } catch (error) {
        await disconnect(error.message || 'Could not talk to the companion');
    }
}

function toggleRecord() {
    state.recording = !state.recording;
    ui.record.setAttribute('aria-pressed', state.recording ? 'true' : 'false');
    ui.record.textContent = state.recording ? 'Stop' : 'Record';
    if (state.recording) {
        state.recordStarted = Date.now();
        setState(state.session ? `Recording · ${state.config?.typeName ?? 'connected'}` : 'Recording', 'rec');
        setError('');
    } else if (state.session) {
        setState(`Connected · ${state.config?.typeName ?? 'companion'}`, 'on');
    } else {
        setState('Disconnected', '');
    }
    renderStats();
}

function clearLog() {
    state.log = [];
    state.recordStarted = state.recording ? Date.now() : 0;
    renderStats();
    setError('');
}

function bindLegend(container, series, hidden) {
    for (const item of series) {
        const button = document.createElement('button');
        button.type = 'button';
        const swatch = document.createElement('i');
        swatch.className = 'swatch';
        swatch.style.background = item.color;
        button.append(swatch, document.createTextNode(item.name));
        button.addEventListener('click', () => {
            if (hidden.has(item.key)) hidden.delete(item.key);
            else hidden.add(item.key);
            button.classList.toggle('off', hidden.has(item.key));
            draw();
        });
        container.append(button);
    }
}

function showSecureContextHelp() {
    const serialOk = typeof navigator !== 'undefined' && 'serial' in navigator;
    if (serialOk && window.isSecureContext) {
        ui.banner.hidden = true;
        return;
    }
    ui.banner.hidden = false;
    ui.connect.disabled = true;
    if (!serialOk) {
        ui.banner.textContent = 'This page talks to the companion with Web Serial. Open it in Chrome or Edge.';
        return;
    }
    ui.banner.textContent = 'Web Serial needs a local address, not a file opened from disk. In this folder run: python -m http.server 8080 then open http://localhost:8080/';
}

ui.connect.addEventListener('click', () => { connect(); });
ui.disconnect.addEventListener('click', () => { disconnect(''); });
ui.record.addEventListener('click', toggleRecord);
ui.clear.addEventListener('click', clearLog);
ui.exportCsv.addEventListener('click', exportCsv);
ui.windowSel.addEventListener('change', draw);
window.addEventListener('resize', draw);
new ResizeObserver(() => draw()).observe(ui.speedCanvas.parentElement);

bindLegend(ui.speedLegend, SPEED_SERIES, state.hiddenSpeed);
bindLegend(ui.pressLegend, PRESS_SERIES, state.hiddenPress);
showSecureContextHelp();
renderStats();
requestAnimationFrame(draw);
