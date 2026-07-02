#!/usr/bin/env python3
"""
Windows 端串口矩阵乘法客户端。

用法:
  python send_matrix.py COM3 --baud 115200

帧格式:
  请求: [0xAA][LEN:u16 LE][ROWS_A][COLS_A][ROWS_B][COLS_B][float32 LE...][CRC8]
  响应: [0xAA][LEN:u16 LE][ROWS][COLS][0x00][0x00][float32 LE...][CRC8]
  错误: [0xAA][0x01 0x00][ERR_CODE][CRC8]

依赖: pip install pyserial
"""

import argparse
import struct
import sys
import time
from typing import Tuple

import serial

HEAD = 0xAA


def _build_crc8_table():
    """CRC-8/MAXIM 查表 (poly=0x31, refined)"""
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 0x01:
                crc = (crc >> 1) ^ 0x8C
            else:
                crc >>= 1
        table.append(crc)
    return table


CRC8_TABLE = _build_crc8_table()


def crc8(data: bytes) -> int:
    crc = 0
    for b in data:
        crc = CRC8_TABLE[(crc ^ b) & 0xFF]
    return crc


def encode_request(a: list[list[float]], b: list[list[float]]) -> bytes:
    """将两个矩阵编码为请求帧"""
    rows_a, cols_a = len(a), len(a[0])
    rows_b, cols_b = len(b), len(b[0])

    payload = bytearray()
    payload.append(rows_a)
    payload.append(cols_a)
    payload.append(rows_b)
    payload.append(cols_b)

    for row in a:
        for v in row:
            payload.extend(struct.pack("<f", v))
    for row in b:
        for v in row:
            payload.extend(struct.pack("<f", v))

    length = len(payload)
    crc_input = struct.pack("<H", length) + payload
    crc = crc8(crc_input)

    frame = bytearray()
    frame.append(HEAD)
    frame.extend(struct.pack("<H", length))
    frame.extend(payload)
    frame.append(crc)
    return bytes(frame)


def decode_response(frame: bytes) -> Tuple[str, list[list[float]] | None, int | None]:
    """解码响应帧，返回 (类型, 矩阵数据或None, 错误码或None)"""
    if len(frame) < 5:
        return ("invalid", None, None)

    if frame[0] != HEAD:
        return ("no_head", None, None)

    length = struct.unpack("<H", frame[1:3])[0]
    payload = frame[3:3 + length]
    crc_byte = frame[3 + length]

    crc_input = frame[1:3] + payload
    expected = crc8(crc_input)
    if expected != crc_byte:
        return ("crc_error", None, None)

    if len(payload) == 1:
        return ("error", None, payload[0])

    if len(payload) < 4:
        return ("invalid", None, None)

    rows = payload[0]
    cols = payload[1]
    data_bytes = payload[4:]
    count = rows * cols
    if len(data_bytes) != count * 4:
        return ("invalid", None, None)

    floats = struct.unpack(f"<{count}f", data_bytes)
    matrix = [[floats[i * cols + j] for j in range(cols)] for i in range(rows)]
    return ("result", matrix, None)


def read_frame(ser: serial.Serial, timeout_ms: int = 2000) -> bytes | None:
    """从串口读取一个完整帧，超时返回 None"""
    deadline = time.monotonic() + timeout_ms / 1000
    buf = bytearray()

    while time.monotonic() < deadline:
        # 等待 HEAD 字节
        while time.monotonic() < deadline:
            b = ser.read(1)
            if not b:
                continue
            if b[0] == HEAD:
                buf.append(b[0])
                break
        if len(buf) < 1:
            continue

        # 读取 LEN (2 bytes)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        ser.timeout = max(0.01, remaining)
        data = ser.read(2)
        if len(data) < 2:
            return None
        buf.extend(data)
        length = struct.unpack("<H", data)[0]

        # 读取 payload + CRC
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        ser.timeout = max(0.01, remaining)
        data = ser.read(length + 1)
        if len(data) < length + 1:
            return None
        buf.extend(data)
        return bytes(buf)

    return None


ERROR_MESSAGES = {
    0x01: "Dimension mismatch: COLS_A != ROWS_B",
    0x02: "CRC verification failed",
    0x03: "Malformed frame",
    0x04: "Data overflow",
}


def print_matrix(name: str, matrix: list[list[float]]):
    print(f"\n{name} =")
    for row in matrix:
        print("  [ " + "  ".join(f"{v:10.4f}" for v in row) + " ]")


def main():
    parser = argparse.ArgumentParser(description="Serial matrix multiplication client")
    parser.add_argument("port", help="Serial port (e.g. COM3)")
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate")
    parser.add_argument("--timeout", type=int, default=2000, help="Read timeout in ms")
    args = parser.parse_args()

    # 示例矩阵
    A = [[1.0, 2.0, 3.0],
         [4.0, 5.0, 6.0]]

    B = [[7.0, 8.0],
         [9.0, 10.0],
         [11.0, 12.0]]

    print_matrix("A", A)
    print_matrix("B", B)
    print(f"\n预期结果 A×B = [[58, 64], [139, 154]]")

    print(f"\n打开串口 {args.port} @ {args.baud}...")
    ser = serial.Serial(
        args.port,
        args.baud,
        timeout=0.1,
        write_timeout=1.0,
        parity=serial.PARITY_NONE,
        stopbits=serial.STOPBITS_ONE,
        bytesize=serial.EIGHTBITS,
        dsrdtr=False,
        rtscts=False,
        xonxoff=False,
    )

    print("发送请求帧...")
    frame = encode_request(A, B)
    print(f"  帧长度: {len(frame)} bytes")
    print(f"  帧数据: {frame.hex(' ')}")
    ser.write(frame)
    ser.flush()

    print("等待响应...")
    response = read_frame(ser, args.timeout)
    if response is None:
        print("超时: 未收到响应")
        sys.exit(1)

    print(f"  响应帧: {response.hex(' ')}")
    typ, matrix, err_code = decode_response(response)

    if typ == "result":
        print_matrix("Result", matrix)
    elif typ == "error":
        msg = ERROR_MESSAGES.get(err_code, f"Unknown error")
        print(f"\n错误: 0x{err_code:02X} - {msg}")
    else:
        print(f"\n解析失败: {typ}")

    ser.close()


if __name__ == "__main__":
    main()