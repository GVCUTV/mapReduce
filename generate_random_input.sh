#!/usr/bin/env bash

# Generate 1,000,000 random signed 64-bit integers uniformly from the full range.
#
# Each 64-bit integer requires 8 bytes.
# For 1,000,000 integers, we need 8,000,000 bytes.

BYTES=8000000
INT64_MAX=9223372036854775807
UINT64_MOD=18446744073709551616 # 2^64

od -An -N${BYTES} -t u8 /dev/urandom | \
awk -v max="$INT64_MAX" -v mod="$UINT64_MOD" '
{
  for (i=1; i<=NF; i++) {
    u=$i
    if (u > max) u = u - mod
    print u
  }
}
' > input
