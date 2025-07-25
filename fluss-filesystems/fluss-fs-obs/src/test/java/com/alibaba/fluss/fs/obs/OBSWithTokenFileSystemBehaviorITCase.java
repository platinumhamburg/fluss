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

package com.alibaba.fluss.fs.obs;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.obs.token.OBSSecurityTokenReceiver;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.UUID;

/** IT case for access obs with iam token in hadoop sdk as FileSystem. */
class OBSWithTokenFileSystemBehaviorITCase extends OBSWithTokenFileSystemBehaviorBaseITCase {

    private static final String TEST_DATA_DIR = "tests-" + UUID.randomUUID();

    @BeforeAll
    static void setup() throws Exception {
        // init a filesystem with ak/sk so that it can generate iam token
        initFileSystemWithSecretKey();
        // now, we can init with iam token
        initFileSystemWithToken(getFsPath());
    }

    @Override
    protected FileSystem getFileSystem() throws Exception {
        return getFsPath().getFileSystem();
    }

    @Override
    protected FsPath getBasePath() {
        return getFsPath();
    }

    protected static FsPath getFsPath() {
        return new FsPath(OBSTestCredentials.getTestBucketUri() + TEST_DATA_DIR);
    }

    @AfterAll
    static void clearFsConfig() {
        FileSystem.initialize(new Configuration(), null);
    }

    private static void initFileSystemWithToken(FsPath fsPath) throws Exception {
        Configuration configuration = new Configuration();
        // obtain a security token and call onNewTokensObtained
        ObtainedSecurityToken obtainedSecurityToken = fsPath.getFileSystem().obtainSecurityToken();
        OBSSecurityTokenReceiver obsSecurityTokenReceiver = new OBSSecurityTokenReceiver();
        obsSecurityTokenReceiver.onNewTokensObtained(obtainedSecurityToken);

        FileSystem.initialize(configuration, null);
    }
}
