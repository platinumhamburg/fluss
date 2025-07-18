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

package com.alibaba.fluss.utils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

import static org.apache.logging.log4j.util.LoaderUtil.findResources;

/**
 * A class loader that blocks resource loading from parent class loader. Designed to simulate SPI
 * loading behavior without delegation to parent.
 */
public class ParentResourceBlockingClassLoader extends URLClassLoader {
    public ParentResourceBlockingClassLoader(URL[] urls) {
        super(urls);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // First, check if the class has already been loaded
            Class<?> c = findLoadedClass(name);
            c = findClass(name);
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        // Skip parent class loader resource loading during Service.load
        return findResources(name);
    }
}
