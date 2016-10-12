package com.fewbytes.statsd;

import com.sun.management.UnixOperatingSystemMXBean;
import org.kohsuke.args4j.Option;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

enum ClientType {BLOCKING, ETSY, NIO, CLQ, DISRUPTOR, DISRUPTOR_LOSSY, CAS, SYNQ, SYNQ_LOSSY }

public class Runner {
    @Option(name = "-host")
    private String host = "localhost";
    @Option(name="-port")
    private int port = 8125;
    @Option(name="-client", required = true)
    private ClientType clientType;
    @Option(name="-threads")
    private int nThreads = 1;

    public void run() throws IOException, InterruptedException {
        IClient client;

        switch (clientType) {
            case BLOCKING:
                client = new BlockingClient(host, port);
                break;
            case ETSY:
                EtsyClient etsyClient = new EtsyClient(host, port);
                etsyClient.enableMultiMetrics(true);
                client = etsyClient;
                break;
            case NIO:
                client = new NIOClient(host, port);
                break;
            case SYNQ:
                client = new SynchronousQueueClient(host, port, false);
                break;
            case SYNQ_LOSSY:
                client = new SynchronousQueueClient(host, port, true);
                break;
            case CLQ:
                client = new ConcurrentLinkedQueueClient(host, port);
                break;
            case CAS:
                client = new com.fewbytes.statsd.cas.AsyncClient(host, port);
                break;
            case DISRUPTOR:
                client = new com.fewbytes.statsd.disruptor.AsyncClient(host, port, false);
                break;
            case DISRUPTOR_LOSSY:
                client = new com.fewbytes.statsd.disruptor.AsyncClient(host, port);
                break;
            default:
                throw new IllegalArgumentException("Unknown client " + clientType);
        }
        System.out.println("Warming up");
        runClient(client, 1000000);

        System.out.println("Starting " + clientType.name() + " test");
        long startCPUTime = getSystemCPUTime();
        long start = System.nanoTime();
        runThreads(client, 1000000, nThreads);
        double duration = (System.nanoTime() - start) / 1E9;
        double consumedCPUTime = (getSystemCPUTime() - startCPUTime) / 1E9;
        System.out.println("Done. Duration: " + duration + ", cpu: " + consumedCPUTime);
        try {
            Method method = client.getClass().getMethod("getLossRatio", (Class<?>) null);
            if (method!=null) {
                double ratio = (double)method.invoke(client, (Object[])null);
                System.out.println("Loss ratio: " + ratio);
            }
        } catch (NoSuchMethodException ignored) {
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    private static void runClient(IClient client, int times) {
        for (int i = 0; i < times; i++) {
            client.incr("test");
        }
    }

    private static void runThreads(IClient client, int times, int nThreads) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++) {
            executorService.submit(() -> runClient(client, times));
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    private long getSystemCPUTime() {
        UnixOperatingSystemMXBean mBean = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return mBean.getProcessCpuTime();
    }
}
