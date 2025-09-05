/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.OutputView;
import org.apache.fluss.utils.MurmurHashUtils;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.apache.fluss.memory.MemoryUtils.UNSAFE;
import static org.apache.fluss.row.BinarySection.HIGHEST_FIRST_BIT;
import static org.apache.fluss.row.BinarySection.HIGHEST_SECOND_TO_EIGHTH_BIT;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Utilities for binary data segments which heavily uses {@link MemorySegment}. */
@Internal
public final class BinarySegmentUtils {

    public static final boolean LITTLE_ENDIAN =
            (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

    private static final int ADDRESS_BITS_PER_WORD = 3;

    private static final int BIT_BYTE_INDEX_MASK = 7;

    /**
     * SQL execution threads is limited, not too many, so it can bear the overhead of 64K per
     * thread.
     */
    private static final int MAX_BYTES_LENGTH = 1024 * 64;

    private static final int MAX_CHARS_LENGTH = 1024 * 32;

    private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final ThreadLocal<byte[]> BYTES_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<char[]> CHARS_LOCAL = new ThreadLocal<>();

    private BinarySegmentUtils() {
        // do not instantiate
    }

    /**
     * Equals two memory segments regions.
     *
     * @param segments1 Segments 1
     * @param offset1 Offset of segments1 to start equaling
     * @param segments2 Segments 2
     * @param offset2 Offset of segments2 to start equaling
     * @param len Length of the equaled memory region
     * @return true if equal, false otherwise
     */
    public static boolean equals(
            MemorySegment[] segments1,
            int offset1,
            MemorySegment[] segments2,
            int offset2,
            int len) {
        if (inFirstSegment(segments1, offset1, len) && inFirstSegment(segments2, offset2, len)) {
            return segments1[0].equalTo(segments2[0], offset1, offset2, len);
        } else {
            return equalsMultiSegments(segments1, offset1, segments2, offset2, len);
        }
    }

    /**
     * Copy segments to a new byte[].
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to copy.
     */
    public static byte[] copyToBytes(MemorySegment[] segments, int offset, int numBytes) {
        return copyToBytes(segments, offset, new byte[numBytes], 0, numBytes);
    }

    /**
     * Copy segments to target byte[].
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param bytes target byte[].
     * @param bytesOffset target byte[] offset.
     * @param numBytes the number bytes to copy.
     */
    public static byte[] copyToBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        if (inFirstSegment(segments, offset, numBytes)) {
            segments[0].get(offset, bytes, bytesOffset, numBytes);
        } else {
            copyMultiSegmentsToBytes(segments, offset, bytes, bytesOffset, numBytes);
        }
        return bytes;
    }

    /**
     * Copy bytes of segments to output view.
     *
     * <p>Note: It just copies the data in, not include the length.
     *
     * @param segments source segments
     * @param offset offset for segments
     * @param sizeInBytes size in bytes
     * @param target target output view
     */
    public static void copyToView(
            MemorySegment[] segments, int offset, int sizeInBytes, OutputView target)
            throws IOException {
        for (MemorySegment sourceSegment : segments) {
            int curSegRemain = sourceSegment.size() - offset;
            if (curSegRemain > 0) {
                int copySize = Math.min(curSegRemain, sizeInBytes);

                byte[] bytes = allocateReuseBytes(copySize);
                sourceSegment.get(offset, bytes, 0, copySize);
                target.write(bytes, 0, copySize);

                sizeInBytes -= copySize;
                offset = 0;
            } else {
                offset -= sourceSegment.size();
            }

            if (sizeInBytes == 0) {
                return;
            }
        }

        if (sizeInBytes != 0) {
            throw new RuntimeException(
                    "No copy finished, this should be a bug, "
                            + "The remaining length is: "
                            + sizeInBytes);
        }
    }

    /**
     * Find equal segments2 in segments1.
     *
     * @param segments1 segs to find.
     * @param segments2 sub segs.
     * @return Return the found offset, return -1 if not find.
     */
    public static int find(
            MemorySegment[] segments1,
            int offset1,
            int numBytes1,
            MemorySegment[] segments2,
            int offset2,
            int numBytes2) {
        if (numBytes2 == 0) { // quick way 1.
            return offset1;
        }
        if (inFirstSegment(segments1, offset1, numBytes1)
                && inFirstSegment(segments2, offset2, numBytes2)) {
            byte first = segments2[0].get(offset2);
            int end = numBytes1 - numBytes2 + offset1;
            for (int i = offset1; i <= end; i++) {
                // quick way 2: equal first byte.
                if (segments1[0].get(i) == first
                        && segments1[0].equalTo(segments2[0], i, offset2, numBytes2)) {
                    return i;
                }
            }
            return -1;
        } else {
            return findInMultiSegments(
                    segments1, offset1, numBytes1, segments2, offset2, numBytes2);
        }
    }

    private static int findInMultiSegments(
            MemorySegment[] segments1,
            int offset1,
            int numBytes1,
            MemorySegment[] segments2,
            int offset2,
            int numBytes2) {
        int end = numBytes1 - numBytes2 + offset1;
        for (int i = offset1; i <= end; i++) {
            if (equalsMultiSegments(segments1, i, segments2, offset2, numBytes2)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * hash segments to int.
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to hash.
     */
    public static int hash(MemorySegment[] segments, int offset, int numBytes) {
        if (inFirstSegment(segments, offset, numBytes)) {
            return MurmurHashUtils.hashBytes(segments[0], offset, numBytes);
        } else {
            return hashMultiSeg(segments, offset, numBytes);
        }
    }

    /**
     * Allocate bytes that is only for temporary usage, it should not be stored in somewhere else.
     * Use a {@link ThreadLocal} to reuse bytes to avoid overhead of byte[] new and gc.
     *
     * <p>If there are methods that can only accept a byte[], instead of a MemorySegment[]
     * parameter, we can allocate a reuse bytes and copy the MemorySegment data to byte[], then call
     * the method. Such as String deserialization.
     */
    public static byte[] allocateReuseBytes(int length) {
        byte[] bytes = BYTES_LOCAL.get();

        if (bytes == null) {
            if (length <= MAX_BYTES_LENGTH) {
                bytes = new byte[MAX_BYTES_LENGTH];
                BYTES_LOCAL.set(bytes);
            } else {
                bytes = new byte[length];
            }
        } else if (bytes.length < length) {
            bytes = new byte[length];
        }

        return bytes;
    }

    public static char[] allocateReuseChars(int length) {
        char[] chars = CHARS_LOCAL.get();

        if (chars == null) {
            if (length <= MAX_CHARS_LENGTH) {
                chars = new char[MAX_CHARS_LENGTH];
                CHARS_LOCAL.set(chars);
            } else {
                chars = new char[length];
            }
        } else if (chars.length < length) {
            chars = new char[length];
        }

        return chars;
    }

    public static void copyMultiSegmentsToBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        int remainSize = numBytes;
        for (MemorySegment segment : segments) {
            int remain = segment.size() - offset;
            if (remain > 0) {
                int nCopy = Math.min(remain, remainSize);
                segment.get(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
                remainSize -= nCopy;
                // next new segment.
                offset = 0;
                if (remainSize == 0) {
                    return;
                }
            } else {
                // remain is negative, let's advance to next segment
                // now the offset = offset - segmentSize (-remain)
                offset = -remain;
            }
        }
    }

    /** Maybe not copied, if want copy, please use copyTo. */
    public static byte[] getBytes(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
        // avoid copy if `base` is `byte[]`
        if (segments.length == 1) {
            byte[] heapMemory = segments[0].getHeapMemory();
            if (baseOffset == 0 && heapMemory != null && heapMemory.length == sizeInBytes) {
                return heapMemory;
            } else {
                byte[] bytes = new byte[sizeInBytes];
                segments[0].get(baseOffset, bytes, 0, sizeInBytes);
                return bytes;
            }
        } else {
            byte[] bytes = new byte[sizeInBytes];
            copyMultiSegmentsToBytes(segments, baseOffset, bytes, 0, sizeInBytes);
            return bytes;
        }
    }

    /** Is it just in first MemorySegment, we use quick way to do something. */
    private static boolean inFirstSegment(MemorySegment[] segments, int offset, int numBytes) {
        return numBytes + offset <= segments[0].size();
    }

    static boolean equalsMultiSegments(
            MemorySegment[] segments1,
            int offset1,
            MemorySegment[] segments2,
            int offset2,
            int len) {
        if (len == 0) {
            // quick way and avoid segSize is zero.
            return true;
        }

        int segSize1 = segments1[0].size();
        int segSize2 = segments2[0].size();

        // find first segIndex and segOffset of segments.
        int segIndex1 = offset1 / segSize1;
        int segIndex2 = offset2 / segSize2;
        int segOffset1 = offset1 - segSize1 * segIndex1; // equal to %
        int segOffset2 = offset2 - segSize2 * segIndex2; // equal to %

        while (len > 0) {
            int equalLen = Math.min(Math.min(len, segSize1 - segOffset1), segSize2 - segOffset2);
            if (!segments1[segIndex1].equalTo(
                    segments2[segIndex2], segOffset1, segOffset2, equalLen)) {
                return false;
            }
            len -= equalLen;
            segOffset1 += equalLen;
            if (segOffset1 == segSize1) {
                segOffset1 = 0;
                segIndex1++;
            }
            segOffset2 += equalLen;
            if (segOffset2 == segSize2) {
                segOffset2 = 0;
                segIndex2++;
            }
        }
        return true;
    }

    /**
     * set bit.
     *
     * @param segment target segment.
     * @param baseOffset bits base offset.
     * @param index bit index from base offset.
     */
    public static void bitSet(MemorySegment segment, int baseOffset, int index) {
        int offset = baseOffset + byteIndex(index);
        byte current = segment.get(offset);
        current |= (1 << (index & BIT_BYTE_INDEX_MASK));
        segment.put(offset, current);
    }

    /**
     * Given a bit index, return the byte index containing it.
     *
     * @param bitIndex the bit index.
     * @return the byte index.
     */
    private static int byteIndex(int bitIndex) {
        return bitIndex >>> ADDRESS_BITS_PER_WORD;
    }

    /**
     * unset bit.
     *
     * @param segment target segment.
     * @param baseOffset bits base offset.
     * @param index bit index from base offset.
     */
    public static void bitUnSet(MemorySegment segment, int baseOffset, int index) {
        int offset = baseOffset + byteIndex(index);
        byte current = segment.get(offset);
        current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
        segment.put(offset, current);
    }

    /**
     * read bit.
     *
     * @param segment target segment.
     * @param baseOffset bits base offset.
     * @param index bit index from base offset.
     */
    public static boolean bitGet(MemorySegment segment, int baseOffset, int index) {
        int offset = baseOffset + byteIndex(index);
        byte current = segment.get(offset);
        return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
    }

    private static int hashMultiSeg(MemorySegment[] segments, int offset, int numBytes) {
        byte[] bytes = allocateReuseBytes(numBytes);
        copyMultiSegmentsToBytes(segments, offset, bytes, 0, numBytes);
        return MurmurHashUtils.hashUnsafeBytes(bytes, BYTE_ARRAY_BASE_OFFSET, numBytes);
    }

    /**
     * get long from segments.
     *
     * @param segments target segments.
     * @param offset value offset.
     */
    public static long getLong(MemorySegment[] segments, int offset) {
        if (inFirstSegment(segments, offset, 8)) {
            return segments[0].getLong(offset);
        } else {
            return getLongMultiSegments(segments, offset);
        }
    }

    private static long getLongMultiSegments(MemorySegment[] segments, int offset) {
        int segSize = segments[0].size();
        int segIndex = offset / segSize;
        int segOffset = offset - segIndex * segSize; // equal to %

        if (segOffset < segSize - 7) {
            return segments[segIndex].getLong(segOffset);
        } else {
            return getLongSlowly(segments, segSize, segIndex, segOffset);
        }
    }

    private static long getLongSlowly(
            MemorySegment[] segments, int segSize, int segNum, int segOffset) {
        MemorySegment segment = segments[segNum];
        long ret = 0;
        for (int i = 0; i < 8; i++) {
            if (segOffset == segSize) {
                segment = segments[++segNum];
                segOffset = 0;
            }
            long unsignedByte = segment.get(segOffset) & 0xff;
            if (LITTLE_ENDIAN) {
                ret |= (unsignedByte << (i * 8));
            } else {
                ret |= (unsignedByte << ((7 - i) * 8));
            }
            segOffset++;
        }
        return ret;
    }

    /**
     * set long from segments.
     *
     * @param segments target segments.
     * @param offset value offset.
     */
    public static void setLong(MemorySegment[] segments, int offset, long value) {
        if (inFirstSegment(segments, offset, 8)) {
            segments[0].putLong(offset, value);
        } else {
            setLongMultiSegments(segments, offset, value);
        }
    }

    private static void setLongMultiSegments(MemorySegment[] segments, int offset, long value) {
        int segSize = segments[0].size();
        int segIndex = offset / segSize;
        int segOffset = offset - segIndex * segSize; // equal to %

        if (segOffset < segSize - 7) {
            segments[segIndex].putLong(segOffset, value);
        } else {
            setLongSlowly(segments, segSize, segIndex, segOffset, value);
        }
    }

    private static void setLongSlowly(
            MemorySegment[] segments, int segSize, int segNum, int segOffset, long value) {
        MemorySegment segment = segments[segNum];
        for (int i = 0; i < 8; i++) {
            if (segOffset == segSize) {
                segment = segments[++segNum];
                segOffset = 0;
            }
            long unsignedByte;
            if (LITTLE_ENDIAN) {
                unsignedByte = value >> (i * 8);
            } else {
                unsignedByte = value >> ((7 - i) * 8);
            }
            segment.put(segOffset, (byte) unsignedByte);
            segOffset++;
        }
    }

    /**
     * Copy target segments from source byte[].
     *
     * @param segments target segments.
     * @param offset target segments offset.
     * @param bytes source byte[].
     * @param bytesOffset source byte[] offset.
     * @param numBytes the number bytes to copy.
     */
    public static void copyFromBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        if (segments.length == 1) {
            segments[0].put(offset, bytes, bytesOffset, numBytes);
        } else {
            copyMultiSegmentsFromBytes(segments, offset, bytes, bytesOffset, numBytes);
        }
    }

    private static void copyMultiSegmentsFromBytes(
            MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
        int remainSize = numBytes;
        for (MemorySegment segment : segments) {
            int remain = segment.size() - offset;
            if (remain > 0) {
                int nCopy = Math.min(remain, remainSize);
                segment.put(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
                remainSize -= nCopy;
                // next new segment.
                offset = 0;
                if (remainSize == 0) {
                    return;
                }
            } else {
                // remain is negative, let's advance to next segment
                // now the offset = offset - segmentSize (-remain)
                offset = -remain;
            }
        }
    }

    /** Gets an instance of {@link Decimal} from underlying {@link MemorySegment}. */
    public static Decimal readDecimalData(
            MemorySegment[] segments,
            int baseOffset,
            long offsetAndSize,
            int precision,
            int scale) {
        final int size = ((int) offsetAndSize);
        int subOffset = (int) (offsetAndSize >> 32);
        byte[] bytes = new byte[size];
        copyToBytes(segments, baseOffset + subOffset, bytes, 0, size);
        return Decimal.fromUnscaledBytes(bytes, precision, scale);
    }

    /**
     * Get binary, if len less than 8, will be include in variablePartOffsetAndLen.
     *
     * <p>Note: Need to consider the ByteOrder.
     *
     * @param baseOffset base offset of composite binary format.
     * @param fieldOffset absolute start offset of 'variablePartOffsetAndLen'.
     * @param variablePartOffsetAndLen a long value, real data or offset and len.
     */
    public static byte[] readBinary(
            MemorySegment[] segments,
            int baseOffset,
            int fieldOffset,
            long variablePartOffsetAndLen) {
        long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
        if (mark == 0) {
            final int subOffset = (int) (variablePartOffsetAndLen >> 32);
            final int len = (int) variablePartOffsetAndLen;
            return BinarySegmentUtils.copyToBytes(segments, baseOffset + subOffset, len);
        } else {
            // Data is stored in fixed-length part (≤7 bytes)
            int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
            byte[] result = new byte[len];

            // Extract the actual data bytes from the low 7 bytes of the long value
            long dataBytes =
                    variablePartOffsetAndLen & 0x00FFFFFFFFFFFFFFL; // Mask out the header byte

            if (BinarySegmentUtils.LITTLE_ENDIAN) {
                // For little endian: byte[i] was stored at position (i * 8)
                for (int i = 0; i < len; i++) {
                    result[i] = (byte) ((dataBytes >>> (i * 8)) & 0xFF);
                }
            } else {
                // For big endian: byte[i] was stored at position ((6 - i) * 8)
                for (int i = 0; i < len; i++) {
                    result[i] = (byte) ((dataBytes >>> ((6 - i) * 8)) & 0xFF);
                }
            }

            return result;
        }
    }

    /**
     * Get binary string, if len less than 8, will be include in variablePartOffsetAndLen.
     *
     * <p>Note: Need to consider the ByteOrder.
     *
     * @param baseOffset base offset of composite binary format.
     * @param fieldOffset absolute start offset of 'variablePartOffsetAndLen'.
     * @param variablePartOffsetAndLen a long value, real data or offset and len.
     */
    public static BinaryString readBinaryString(
            MemorySegment[] segments,
            int baseOffset,
            int fieldOffset,
            long variablePartOffsetAndLen) {
        long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
        if (mark == 0) {
            final int subOffset = (int) (variablePartOffsetAndLen >> 32);
            final int len = (int) variablePartOffsetAndLen;
            return BinaryString.fromAddress(segments, baseOffset + subOffset, len);
        } else {
            int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
            if (BinarySegmentUtils.LITTLE_ENDIAN) {
                return BinaryString.fromAddress(segments, fieldOffset, len);
            } else {
                // fieldOffset + 1 to skip header.
                return BinaryString.fromAddress(segments, fieldOffset + 1, len);
            }
        }
    }

    /**
     * hash segments to int, numBytes must be aligned to 4 bytes.
     *
     * @param segments Source segments.
     * @param offset Source segments offset.
     * @param numBytes the number bytes to hash.
     */
    public static int hashByWords(MemorySegment[] segments, int offset, int numBytes) {
        if (inFirstSegment(segments, offset, numBytes)) {
            return MurmurHashUtils.hashBytesByWords(segments[0], offset, numBytes);
        } else {
            return hashMultiSegByWords(segments, offset, numBytes);
        }
    }

    private static int hashMultiSegByWords(MemorySegment[] segments, int offset, int numBytes) {
        byte[] bytes = allocateReuseBytes(numBytes);
        copyMultiSegmentsToBytes(segments, offset, bytes, 0, numBytes);
        return MurmurHashUtils.hashUnsafeBytesByWords(bytes, BYTE_ARRAY_BASE_OFFSET, numBytes);
    }

    /**
     * Gets an instance of {@link TimestampLtz} from underlying {@link MemorySegment}.
     *
     * @param segments the underlying MemorySegments
     * @param baseOffset the base offset of current instance of {@code TimestampLtz}
     * @param offsetAndNanos the offset of milli-seconds part and nanoseconds
     * @return an instance of {@link TimestampLtz}
     */
    public static TimestampLtz readTimestampLtzData(
            MemorySegment[] segments, int baseOffset, long offsetAndNanos) {
        final int nanoOfMillisecond = (int) offsetAndNanos;
        final int subOffset = (int) (offsetAndNanos >> 32);
        final long millisecond = getLong(segments, baseOffset + subOffset);
        return TimestampLtz.fromEpochMillis(millisecond, nanoOfMillisecond);
    }

    /**
     * Gets an instance of {@link TimestampNtz} from underlying {@link MemorySegment}.
     *
     * @param segments the underlying MemorySegments
     * @param baseOffset the base offset of current instance of {@code TimestampNtz}
     * @param offsetAndNanos the offset of milli-seconds part and nanoseconds
     * @return an instance of {@link TimestampNtz}
     */
    public static TimestampNtz readTimestampNtzData(
            MemorySegment[] segments, int baseOffset, long offsetAndNanos) {
        final int nanoOfMillisecond = (int) offsetAndNanos;
        final int subOffset = (int) (offsetAndNanos >> 32);
        final long millisecond = getLong(segments, baseOffset + subOffset);
        return TimestampNtz.fromMillis(millisecond, nanoOfMillisecond);
    }
}
