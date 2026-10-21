// Happy Cactus Garage MK60 companion serial protocol.
// Framing matches ProtobufCommand.ts: one length byte (payload length - 1),
// then a protobuf Request/Response. Baud rate is 115200.
// Field numbers match generated/wss_pb.js.

export class TimeoutError extends Error {
    constructor(message) {
        super(message);
        this.name = 'TimeoutError';
    }
}

export const ABS_TYPE_NAMES = ['MK60', 'MK60E1', 'MK60E5', 'Generic on/off'];

export const ABS_BRAKE_SWITCH = ['off', 'on', 'switch fault', 'invalid'];
export const ABS_BRAKING_STATE = ['not braking', 'braking', 'switch invalid', 'switch and pressure invalid'];
export const ST_ABS = ['normal', 'fallback level', 'malfunction', 'invalid'];
export const ST_DSC = ['OK', 'DSC off', 'DSC fault', 'steering angle invalid', 'DTC / M mode', 'ABS fault', 'startup / sensor problem', 'invalid'];

export function label(table, value) {
    return table[value] ?? `unknown (${value})`;
}

function encodeVarint(value) {
    let v = value >>> 0;
    const out = [];
    while (v >= 0x80) {
        out.push((v & 0x7f) | 0x80);
        v >>>= 7;
    }
    out.push(v);
    return out;
}

function encodeKey(field, wire) {
    return encodeVarint((field << 3) | wire);
}

function concatBytes(parts) {
    const arrays = parts.map((part) => (part instanceof Uint8Array ? part : Uint8Array.from(part)));
    const length = arrays.reduce((sum, part) => sum + part.length, 0);
    const out = new Uint8Array(length);
    let offset = 0;
    for (const part of arrays) {
        out.set(part, offset);
        offset += part.length;
    }
    return out;
}

/** Length byte is payload.length - 1, same as the config tool. */
export function framePayload(body) {
    const payload = body instanceof Uint8Array ? body : Uint8Array.from(body);
    if (payload.length < 1 || payload.length > 256) {
        throw new Error(`Protobuf payload length ${payload.length} does not fit in one length byte`);
    }
    const framed = new Uint8Array(payload.length + 1);
    framed[0] = payload.length - 1;
    framed.set(payload, 1);
    return framed;
}

function emptyCommand(cmdId, fieldNumber) {
    return framePayload(concatBytes([
        encodeKey(1, 0),
        encodeVarint(cmdId),
        encodeKey(fieldNumber, 2),
        [0],
    ]));
}

export const buildIdentify = (cmdId) => emptyCommand(cmdId, 12);
export const buildGetConfig = (cmdId) => emptyCommand(cmdId, 14);
export const buildGetStatus = (cmdId) => emptyCommand(cmdId, 16);

function readVarint32(buf, offset) {
    let result = 0;
    let shift = 0;
    let i = offset;
    while (i < buf.length) {
        const byte = buf[i++];
        if (shift < 32) {
            result += (byte & 0x7f) * (2 ** shift);
        }
        if ((byte & 0x80) === 0) {
            const unsigned = result % 4294967296;
            const signed = unsigned >= 2147483648 ? unsigned - 4294967296 : unsigned;
            return { unsigned, signed, next: i };
        }
        shift += 7;
        if (shift > 70) {
            throw new Error('Varint is too long');
        }
    }
    throw new Error('Truncated varint');
}

export function decodeFields(buffer) {
    const bytes = buffer instanceof Uint8Array ? buffer : Uint8Array.from(buffer);
    const fields = [];
    let i = 0;
    while (i < bytes.length) {
        const tag = readVarint32(bytes, i);
        i = tag.next;
        const field = tag.unsigned >>> 3;
        const wire = tag.unsigned & 7;
        if (wire === 0) {
            const value = readVarint32(bytes, i);
            i = value.next;
            fields.push({ field, wire, unsigned: value.unsigned, signed: value.signed });
        } else if (wire === 2) {
            const length = readVarint32(bytes, i);
            i = length.next;
            const end = i + length.unsigned;
            if (end > bytes.length) {
                throw new Error('Truncated length-delimited field');
            }
            fields.push({ field, wire, bytes: bytes.subarray(i, end) });
            i = end;
        } else if (wire === 5) {
            i += 4;
        } else if (wire === 1) {
            i += 8;
        } else {
            throw new Error(`Unsupported protobuf wire type ${wire}`);
        }
    }
    return fields;
}

function fieldMap(fields) {
    const map = new Map();
    for (const field of fields) {
        map.set(field.field, field);
    }
    return map;
}

function uintField(map, number) {
    return map.get(number)?.unsigned ?? 0;
}

function intField(map, number) {
    return map.get(number)?.signed ?? 0;
}

function boolField(map, number) {
    return (map.get(number)?.unsigned ?? 0) !== 0;
}

export function decodeIdent(buffer) {
    const map = fieldMap(decodeFields(buffer));
    const serial = map.get(6)?.bytes ?? new Uint8Array();
    return {
        bootloader: boolField(map, 1),
        version: intField(map, 2),
        hardwareRevision: intField(map, 3),
        appStart: uintField(map, 4),
        appSize: uintField(map, 5),
        serialHex: [...serial].map((byte) => byte.toString(16).padStart(2, '0')).join(''),
        bluetooth: boolField(map, 7),
        hallVr: boolField(map, 8),
    };
}

export function decodeConfig(buffer) {
    const map = fieldMap(decodeFields(buffer));
    const type = uintField(map, 1);
    return {
        type,
        typeName: ABS_TYPE_NAMES[type] ?? `type ${type}`,
        dirLf: boolField(map, 2),
        dirRf: boolField(map, 3),
        dirLr: boolField(map, 4),
        dirRr: boolField(map, 5),
        speedoPulsePerKm: uintField(map, 6),
    };
}

export function decodeStatus(buffer) {
    const map = fieldMap(decodeFields(buffer));
    return {
        wss: [intField(map, 1), intField(map, 2), intField(map, 3), intField(map, 4)],
        wssInvalid: [boolField(map, 24), boolField(map, 25), boolField(map, 26), boolField(map, 27)],
        press: [intField(map, 5), intField(map, 6), intField(map, 7), intField(map, 8)],
        pressInput: intField(map, 18),
        vehicle: intField(map, 14),
        vehicleInvalid: boolField(map, 28),
        tempC: intField(map, 9),
        brakeSwitch: boolField(map, 10),
        canRx: uintField(map, 11),
        canTxOk: uintField(map, 12),
        canTxErr: uintField(map, 13),
        faultLight: boolField(map, 15),
        absBrakeSwitchState: boolField(map, 17),
        absBrakeSwitch: uintField(map, 20),
        absBrakingState: uintField(map, 21),
        stAbs: uintField(map, 22),
        stDsc: uintField(map, 23),
    };
}

export function decodeResponse(payload) {
    const map = fieldMap(decodeFields(payload));
    const response = { cmdId: uintField(map, 1) };
    if (map.has(10)) {
        response.ack = true;
    }
    if (map.has(11)) {
        const nak = fieldMap(decodeFields(map.get(11).bytes));
        const message = nak.get(1)?.bytes;
        response.nak = message ? new TextDecoder().decode(message) : '';
    }
    if (map.has(12)) {
        response.ident = decodeIdent(map.get(12).bytes);
    }
    if (map.has(13)) {
        response.config = decodeConfig(map.get(13).bytes);
    }
    if (map.has(14)) {
        response.status = decodeStatus(map.get(14).bytes);
    }
    return response;
}

export function splitFrame(framed) {
    const bytes = framed instanceof Uint8Array ? framed : Uint8Array.from(framed);
    if (bytes.length < 2) {
        throw new Error('Frame is too short');
    }
    const payloadLength = bytes[0] + 1;
    if (bytes.length !== payloadLength + 1) {
        throw new Error(`Frame length byte says ${payloadLength} payload bytes, buffer has ${bytes.length - 1}`);
    }
    return decodeResponse(bytes.subarray(1));
}

export function toHex(bytes) {
    return [...bytes].map((byte) => byte.toString(16).padStart(2, '0')).join(' ');
}
