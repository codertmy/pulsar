/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.metadata.impl;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.Stat;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MetadataStoreTmyTest {
    public static final String LETTER_CHAR = "abcdefghijkllmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public static final String basePath = "/tmy/press/test/persistent";
    public static final String topicPrefix = "/iot-pulsar-press-abcdefghijklABCDEFGHIJKL-";
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    private static ExecutorService executor;
    private static String  zkUrl;
    private static int zNodeCount;
    private static int testOpCount;
    private static int batchMaxOp;
    private static int batchMaxMillis;
    private static int threadPool;
    private static MetadataStore store;
    private static long start;
    private static RateLimiter rateLimiter;
    private static int createRate;
    private static int readRate;
    private static int writeRate;
    private static int getChildrenRate;
    private static int mixRate;

    public static String generateUpperLowerString(int length) {
        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(LETTER_CHAR.charAt(random.nextInt(LETTER_CHAR.length())));
        }
        return sb.toString();
    }

    private enum OpType {
        PUT,
        GET,
        GETCHILDREN
    }

    public static void main(String[] args) throws Exception {
        zkUrl = System.getProperty("zkUrl", "localhost:2181");
        zNodeCount = Integer.parseInt(System.getProperty("zNodeCount", "1"));
        testOpCount = Integer.parseInt(System.getProperty("testOpCount", "1"));
        batchMaxOp = Integer.parseInt(System.getProperty("batchMaxOp", "1000"));
        batchMaxMillis = Integer.parseInt(System.getProperty("batchMaxMillis", "5"));
        threadPool = Integer.parseInt(System.getProperty("threadPool", "1000"));
        store = MetadataStoreFactory.create(zkUrl,
                MetadataStoreConfig.builder()
                        .batchingMaxOperations(batchMaxOp)
                        .batchingMaxDelayMillis(batchMaxMillis)
                        .build());
        executor = Executors.newFixedThreadPool(threadPool, new DefaultThreadFactory("zk op thread"));

        createRate = Integer.parseInt(System.getProperty("createRate", "10000"));
        readRate = Integer.parseInt(System.getProperty("readRate", "30000"));
        writeRate = Integer.parseInt(System.getProperty("writeRate", "10000"));
        getChildrenRate = Integer.parseInt(System.getProperty("getChildrenRate", "80000"));
        mixRate = Integer.parseInt(System.getProperty("mixRate", "20000"));

        System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Start create node test, opCount:" + zNodeCount);
        start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(zNodeCount);
        rateLimiter = RateLimiter.create(createRate);
        create(countDownLatch, rateLimiter);
        countDownLatch.await();
        System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Finish create node test, opCount:" + zNodeCount + ", cost:" + (System.currentTimeMillis() - start) + "ms");
        System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " ---------------------------------------------------------------------------------------------------------");


        while (testOpCount <= 10000000) {
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Start test zk op, test op count:" + testOpCount);

            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Start only read test, opCount:" + testOpCount);
            countDownLatch = new CountDownLatch(testOpCount);
            rateLimiter = RateLimiter.create(readRate);
            long start = System.currentTimeMillis();
            onlyReadTest(testOpCount, countDownLatch, rateLimiter);
            countDownLatch.await();
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + sdf.format(new Date()) + " Finish only read test, opCount:" + testOpCount + ", cost:" + (System.currentTimeMillis() - start) + "ms");
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " ---------------------------------------------------------------------------------------------------------");

            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Start only write test, opCount:" + testOpCount);
            countDownLatch = new CountDownLatch(testOpCount);
            rateLimiter = RateLimiter.create(writeRate);
            start = System.currentTimeMillis();
            onlyWriteTest(testOpCount, countDownLatch, rateLimiter);
            countDownLatch.await();
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + sdf.format(new Date()) + " Finish only write test, opCount:" + testOpCount + ", cost:" + (System.currentTimeMillis() - start) + "ms");
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " ---------------------------------------------------------------------------------------------------------");

/*
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Start only get children test, opCount:" + testOpCount);
            countDownLatch = new CountDownLatch(testOpCount);
            rateLimiter = RateLimiter.create(getChildrenRate);
            start = System.currentTimeMillis();
            onlyGetChildren(testOpCount, countDownLatch, rateLimiter);
            countDownLatch.await();
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + sdf.format(new Date()) + " Finish only get children test, opCount:" + testOpCount + ", cost:" + (System.currentTimeMillis() - start) + "ms");
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " ---------------------------------------------------------------------------------------------------------");
*/

            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Start mix test, opCount:" + testOpCount);
            countDownLatch = new CountDownLatch(testOpCount);
            rateLimiter = RateLimiter.create(mixRate);
            start = System.currentTimeMillis();
            mixTest(testOpCount, countDownLatch, rateLimiter);
            countDownLatch.await();
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + sdf.format(new Date()) + " Finish mix test, opCount:" + testOpCount + ", cost:" + (System.currentTimeMillis() - start) + "ms");
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " ---------------------------------------------------------------------------------------------------------");

            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " Finish  test zk op, test op count:" + testOpCount);
            System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " *********************************************************************************************************");
            testOpCount *= 10;
        }
    }

    private static void create(CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        for (int i = 0; i < zNodeCount; ++i) {
            int index = i;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    rateLimiter.acquire();
                    String zkPath = basePath + topicPrefix + index;
                    storeOP(OpType.PUT, zkPath, store);
                    countDownLatch.countDown();
                }
            });
        }
    }

    private static void onlyReadTest(int testCount, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        for (int i = 0; i < testCount; ++i) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    rateLimiter.acquire();
                    Random random = new Random();
                    int index = random.nextInt(zNodeCount);
                    String zkPath = basePath + topicPrefix + index;
                    storeOP(OpType.GET, zkPath, store);
                    countDownLatch.countDown();
                }
            });
        }
    }

    private static void onlyWriteTest(int testCount, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        for (int i = 0; i < testOpCount; ++i) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    rateLimiter.acquire();
                    Random random = new Random();
                    int index = random.nextInt(zNodeCount);
                    String zkPath = basePath + topicPrefix + index;
                    storeOP(OpType.PUT, zkPath, store);
                    countDownLatch.countDown();
                }
            });
        }
    }

    private static void onlyGetChildren(int testCount, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        for (int i = 0; i < testCount; ++i) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    rateLimiter.acquire();
                    storeOP(OpType.GETCHILDREN, basePath, store);
                    countDownLatch.countDown();
                }
            });
        }
    }

    private static void mixTest(int testCount, CountDownLatch countDownLatch, RateLimiter rateLimiter) {
        for (int i = 0; i < testCount; ++i) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    rateLimiter.acquire();
                    Random random = new Random();
                    int index = random.nextInt(zNodeCount);
                    String zkPath = basePath + topicPrefix + index;
                    if (index % 2 == 0) {
                        storeOP(OpType.PUT, zkPath, store);
                    } else {
                        storeOP(OpType.GET, zkPath, store);
                    } /*else {
                        storeOP(OpType.GETCHILDREN, basePath, store);
                    }*/
                    countDownLatch.countDown();
                }
            });
        }

    }

    private static void storeOP(OpType opType, String zkPath, MetadataStore store) {
        switch (opType) {
            case GET:
                try {
                    Optional<GetResult> getResult = store.get(zkPath).get();
                    if (getResult.isPresent()) {
                        System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", read success, result:" + getResult);
                    } else {
                        System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", read fail, result null");
                    }
                } catch (Exception ex) {
                    System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", read fail, exception:" + ex);
                }
                break;
            case PUT:
                try {
                    boolean exist = store.exists(zkPath).get();
                    long version = exist ? store.get(zkPath).get().get().getStat().getVersion() : -1;
                    store.put(zkPath, generateUpperLowerString(52).getBytes(), Optional.of(version)).get();
                    System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", write success");
                } catch (Exception ex) {
                    System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", write fail, exception:" + ex);
                }
                break;
            case GETCHILDREN:
                try {
                    List<String> topicList = store.getChildren(zkPath).get();
                    System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", mix get children success, topicCount:" + topicList.size());
                } catch (Exception ex) {
                    System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + " zk path: " + zkPath + ", mix get children fail, exception:" + ex);
                }
                break;
            default:
                System.out.println(sdf.format(new Date()) + " Thread:" + Thread.currentThread().getName() + ", ZK op is error");
        }
    }
}
