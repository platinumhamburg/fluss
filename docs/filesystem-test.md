<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Filesystem test client

`TestFileSystemClient` accesses the dedicated test server's configured remote
storage through the existing Fluss connection. The client and server must both
come from `release-0.9-ali-7.0-rc3-filesystem-test`. This branch is for isolated test
instances only and must not be merged into public or production release branches.

## Operations

| Method | Behavior |
| --- | --- |
| `list("")` | Returns configured remote roots, including the legacy root. Root discovery does not require a directory marker; these entries use length 0 and unavailable mtime (`Long.MAX_VALUE`). |
| `list(path)` | Lists direct children using the server filesystem. |
| `stat(path)` | Returns path, directory flag, length and filesystem mtime. |
| `read(path, offset, length)` | Reads up to length bytes; returns fewer bytes at EOF. Maximum length: 1 MiB. |
| `write(path, content, overwrite)` | Writes an entire file, up to 1 MiB; false rejects an existing destination. |
| `delete(path, recursive)` | Returns the filesystem's boolean result; failures remain exceptional. |
| `copy(source, target, overwrite)` | Streams a file on the server using a 64 KiB buffer; supports different configured roots and files larger than 1 MiB. |

Use complete filesystem URIs returned by `list` or `stat`. Paths must remain
inside configured roots. Relative path segments, credentials, query strings and
fragments are rejected. Root directories cannot be overwritten or deleted. Copy
requires a file source and a different destination. The endpoint uses the existing
cluster `ALTER` permission when authorization is enabled.

All methods return `CompletableFuture`. Missing files, access denial and other I/O
failures propagate as `FileNotFoundException`, `AccessDeniedException` and
`IOException` causes. Invalid arguments produce `IllegalArgumentException`.
Transport and Fluss authorization failures use the existing RPC exceptions.

Writes and copies return only after the output stream closes. They are not
transactional: an I/O failure may leave a partial target, depending on the
filesystem. No mutation is automatically retried. After a timeout, inspect the
target before retrying. Copy duration is subject to the normal client RPC timeout.

OSS mtime cannot be backdated. A write or copy does not preserve the source mtime;
files used for age-based cleanup must age at their destination. Directory semantics
are those of the configured filesystem adapter. Listing is not a snapshot across
concurrent operations, and large directories must fit the existing RPC response
and memory limits.

## Example

Save this example as `FilesystemTestExample.java`. Pass the address of an isolated
test instance. For an authenticated instance, set the same authentication options
on `conf` that are used by its normal Fluss client.

```java
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.TestFileSystemClient;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.UUID;

public class FilesystemTestExample {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(ConfigOptions.BOOTSTRAP_SERVERS, Collections.singletonList(args[0]));
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            TestFileSystemClient fs = new TestFileSystemClient(connection);
            String root = fs.list("").get().get(0).getPath();
            String directory = root + "/filesystem-example-" + UUID.randomUUID();
            String source = directory + "/source";
            String target = directory + "/target";
            try {
                fs.write(source, "sample".getBytes(StandardCharsets.UTF_8), false).get();
                fs.copy(source, target, false).get();
                System.out.println(fs.list(directory).get());
                System.out.println(fs.stat(target).get());
                System.out.println(new String(fs.read(target, 0, 1024).get(), StandardCharsets.UTF_8));
                fs.delete(target, false).get();
            } finally {
                fs.delete(directory, true).get();
            }
        }
    }
}
```

Build the client and server with the repository's Maven wrapper:

```bash
./mvnw -pl fluss-client -am -DskipTests install
javac -cp fluss-client/target/fluss-client-0.9-ali-7.0-rc3.jar FilesystemTestExample.java
java -cp .:fluss-client/target/fluss-client-0.9-ali-7.0-rc3.jar FilesystemTestExample localhost:9123
```

The example creates arbitrary files only. To create cleanup candidates, use paths
and file contents consistent with the actual table/snapshot layout; obtain valid
SST and log files from normal Fluss writes, then copy them to the desired paths.
