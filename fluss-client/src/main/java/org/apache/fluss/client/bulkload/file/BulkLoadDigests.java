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

package org.apache.fluss.client.bulkload.file;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** SHA-256 helpers for BulkLoad input files and batches. */
final class BulkLoadDigests {

    private BulkLoadDigests() {}

    /** Returns the lowercase 64-char hexadecimal SHA-256 of the given bytes. */
    static String sha256Hex(byte[] bytes) {
        return toHex(newDigest().digest(bytes));
    }

    static String sha256Hex(Path path) throws IOException {
        MessageDigest digest = newDigest();
        try (InputStream input =
                Channels.newInputStream(
                        Files.newByteChannel(
                                path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))) {
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return toHex(digest.digest());
    }

    static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required but unavailable.", e);
        }
    }

    static String toHex(byte[] hash) {
        StringBuilder value = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            value.append(String.format("%02x", b & 0xff));
        }
        return value.toString();
    }
}
