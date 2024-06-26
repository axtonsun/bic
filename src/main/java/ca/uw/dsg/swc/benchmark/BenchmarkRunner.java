package ca.uw.dsg.swc.benchmark;

import ca.uw.dsg.swc.AbstractSlidingWindowConnectivity;
import ca.uw.dsg.swc.StreamingEdge;
import ca.uw.dsg.swc.baselines.FdcSlidingWindowConnectivity;
import ca.uw.dsg.swc.baselines.dtree.DTreeConnectivity;
import ca.uw.dsg.swc.baselines.etr.EtrConnectivity;
import ca.uw.dsg.swc.baselines.hdt.HdtConnectivity;
import ca.uw.dsg.swc.baselines.naive.DfsConnectivity;
import ca.uw.dsg.swc.baselines.naive.RecalculatingWindowConnectivity;
import ca.uw.dsg.swc.bic.BidirectionalIncrementalConnectivity;
import it.unimi.dsi.fastutil.ints.IntIntPair;
import org.jgrapht.alg.util.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BenchmarkRunner {
    static final String BENCHMARK_RESULTS = "./benchmark/results/";
    static final String BENCHMARK_DATASETS = "./benchmark/datasets/";
    static final String BENCHMARK_WORKLOADS = "./benchmark/workloads/";

    static final Map<String, Integer> GRAPH_VERTEX_NUM = Map.of(
            "sg-wiki-topcats", 1791489,
            "sg-com-lj.ungraph", 3997962,
            "sg-youtube-u-growth", 3223589,
            "sg-soc-pokec-relationships", 1632803,
            "sg-stackoverflow", 2601977,
            "sg-orkut", 3072441,
            "sg-ldbc-sf1k-knows", 3298534,
            "sg-graph500-25", 17062472,
            "sg-com-friendster.ungraph", 65608366,
            "sg-semantic-scholar", 65695514
    );

    public static void main(String[] args) {
        // performance evaluation
        throughputRunner();
        latencyRunner();

        // fixed slide interval, varied window sizes
        // scalabilityFixedSlideThrExpRunner();
        // scalabilityFixedSlideLatencyExpRunner();

        // fixed window size, varied slide intervals
        // scalabilityFixedRangeThrExpRunner();
        // scalabilityFixedRangeLatencyExpRunner();

        // varied workload sizes
        // scalabilityWorkloadThrExpRunner();
        // scalabilityWorkloadLatencyExpRunner();

        // memory consumption
        memoryConsumptionRunner();
        // scalabilityFixedSlideMemRunner();
        // scalabilityFixedRangeMemRunner();
    }

    private static void throughputRunner() {
        List<String> results = new ArrayList<>();
        int repeat = 6; // 重复6次
        String[] methods = { // 5种方法
                "D-Tree",
                "RWC",
                "BIC",
                "ET-Tree",
                "HDT"
        };
        // 传入的参数分别为：D-Tree, per-eva, sg-wiki-topcats, 10小时30分钟, 6次, results
        setupThrExp(
                methods,
                "per-eva",
                "sg-wiki-topcats", // WT
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))), // 将小时数乘以3600来计算总秒数，将分钟数乘以60来计算总秒数
                repeat,
                results
        );
        setupThrExp(
                methods,
                "per-eva",
                "sg-soc-pokec-relationships", // PR: 685.75 MB
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
                repeat,
                results
        );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-com-lj.ungraph", // LJ: 798.47 MB
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-stackoverflow", // SO: 1.70 GB
        //         List.of(Pair.of(Duration.ofDays(180), Duration.ofDays(9))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-orkut", // OR: 2.74 GB
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-ldbc-sf1k-knows", // LK(synthetic): 5.04 GB
        //         List.of(Pair.of(Duration.ofDays(20), Duration.ofDays(1))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-graph500-25", // GF(synthetic): 13.71 GB
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-com-friendster.ungraph", // FS: 49.29 GB
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-semantic-scholar", // SC: 
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
        //         repeat,
        //         results
        // );
        // setupThrExp(
        //         methods,
        //         "per-eva",
        //         "sg-youtube-u-growth", // YG:
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30))),
        //         repeat,
        //         results
        // );
        writeResult(results, BENCHMARK_RESULTS + "throughput-per-eva-" + LocalDateTime.now() + ".txt");
    }

    private static void latencyRunner() {
        String expType = "per-eva";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC",
                "ET-Tree",
                "HDT"
        };
        setupLatencyExp(
                methods,
                expType,
                "sg-wiki-topcats",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-soc-pokec-relationships",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-com-lj.ungraph",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-stackoverflow",
        //         List.of(Pair.of(Duration.ofDays(180), Duration.ofDays(9)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-orkut",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-ldbc-sf1k-knows",
        //         List.of(Pair.of(Duration.ofDays(20), Duration.ofDays(1)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-graph500-25",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-com-friendster.ungraph",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-semantic-scholar",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupLatencyExp(
        //         methods,
        //         expType,
        //         "sg-youtube-u-growth",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
    }

    private static void scalabilityFixedSlideThrExpRunner() {
        List<String> results = new ArrayList<>();
        int repeat = 6;
        String expType = "fixed-slide";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupThrExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                ),
                repeat,
                results
        );

        setupThrExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                ),
                repeat,
                results
        );

        setupThrExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                ),
                repeat,
                results
        );

        writeResult(results, BENCHMARK_RESULTS + "throughput-" + expType + "-" + LocalDateTime.now() + ".txt");
    }

    private static void scalabilityFixedSlideLatencyExpRunner() {
        String expType = "fixed-slide";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );
    }

    private static void scalabilityFixedRangeThrExpRunner() {
        List<String> results = new ArrayList<>();
        int repeat = 6;
        String expType = "fixed-range";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupThrExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                ),
                repeat,
                results
        );

        setupThrExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                ),
                repeat,
                results
        );

        setupThrExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                ),
                repeat,
                results
        );
        writeResult(results, BENCHMARK_RESULTS + "throughput-" + expType + "-" + LocalDateTime.now() + ".txt");
    }

    private static void scalabilityFixedRangeLatencyExpRunner() {
        String expType = "fixed-range";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };
        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );

        setupLatencyExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );
    }

    private static void scalabilityWorkloadThrExpRunner() {
        List<String> results = new ArrayList<>();

        String expType = "workload";
        int repeat = 6;
        int[] sizes = new int[]{1, 10, 100, 1000, 10000};
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupThrExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-graph500-25", sizes)
        );
        setupThrExp(
                new String[]{"DFS"},
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-graph500-25", new int[]{1, 10})
        );
        setupThrExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-com-friendster.ungraph", sizes)
        );
        setupThrExp(
                new String[]{"DFS"},
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-com-friendster.ungraph", new int[]{1, 10})
        );
        setupThrExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-com-friendster.ungraph", sizes)
        );
        setupThrExp(
                new String[]{"DFS"},
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                repeat,
                results,
                getWorkLoads("sg-com-friendster.ungraph", new int[]{1, 10})
        );

        writeResult(results, BENCHMARK_RESULTS + "throughput-" + expType + "-" + LocalDateTime.now() + ".txt");
    }

    private static void scalabilityWorkloadLatencyExpRunner() {
        String expType = "workload";
        int[] sizes = new int[]{1, 10, 100, 1000, 10000};
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupLatencyExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-graph500-25", sizes)
        );
        setupLatencyExp(
                new String[]{"DFS"},
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-graph500-25", new int[]{1, 10})
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-com-friendster.ungraph", sizes)
        );
        setupLatencyExp(
                new String[]{"DFS"},
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-com-friendster.ungraph", new int[]{1, 10})
        );
        setupLatencyExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-com-friendster.ungraph", sizes)
        );
        setupLatencyExp(
                new String[]{"DFS"},
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3))
                ),
                getWorkLoads("sg-com-friendster.ungraph", new int[]{1, 10})
        );

    }

    private static void memoryConsumptionRunner() {
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC",
                "ET-Tree",
                "HDT"
        };
        setupMemExp(
                methods,
                "per-eva",
                "sg-wiki-topcats",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-youtube-u-growth",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        setupMemExp(
                methods,
                "per-eva",
                "sg-soc-pokec-relationships",
                List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-com-lj.ungraph",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-stackoverflow",
        //         List.of(Pair.of(Duration.ofDays(180), Duration.ofDays(9)))
        // );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-orkut",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );

        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-ldbc-sf1k-knows",
        //         List.of(Pair.of(Duration.ofDays(20), Duration.ofDays(1)))
        // );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-graph500-25",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-com-friendster.ungraph",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
        // setupMemExp(
        //         methods,
        //         "per-eva",
        //         "sg-semantic-scholar",
        //         List.of(Pair.of(Duration.ofHours(10), Duration.ofMinutes(30)))
        // );
    }

    private static void scalabilityFixedSlideMemRunner() {
        String expType = "fixed-slide";

        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };

        setupMemExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );
        setupMemExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );
        setupMemExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 10), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 20), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 40), Duration.ofHours(3)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3))
                )
        );
    }

    private static void scalabilityFixedRangeMemRunner() {
        String expType = "fixed-range";
        String[] methods = {
                "D-Tree",
                "RWC",
                "BIC"
        };
        setupMemExp(
                methods,
                expType,
                "sg-graph500-25",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );
        setupMemExp(
                methods,
                expType,
                "sg-com-friendster.ungraph",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );
        setupMemExp(
                methods,
                expType,
                "sg-semantic-scholar",
                List.of(
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 2)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 4)),
                        Pair.of(Duration.ofHours(3 * 80), Duration.ofHours(3 * 8))
                )
        );
    }

    private static void setupMemExp( // various sizes of workloads
                                     String[] methods,
                                     String expType,
                                     String graph,
                                     List<Pair<Duration, Duration>> rangeAndSlides) {

        // get graph
        List<StreamingEdge> streamingEdges = GraphUtils.readStreamingGraph(BENCHMARK_DATASETS + graph + ".txt", ",");

        if (rangeAndSlides == null)
            return;

        System.out.println("Range and slide" + rangeAndSlides);
        List<IntIntPair> workloads = getWorkLoad(graph, 100);
        for (String method : methods)
            runMemExp(
                    graph,
                    method,
                    expType,
                    rangeAndSlides,
                    workloads,
                    streamingEdges
            );
    }

    private static void runMemExp(
            String graph,
            String method,
            String expType,
            List<Pair<Duration, Duration>> rangeSlides,
            List<IntIntPair> workload,
            List<StreamingEdge> streamingEdges) {
        System.out.println("Start " + expType + " memory experiments for " + method + " on " + graph + " with ranges and slides of " + rangeSlides);
        for (Pair<Duration, Duration> rangeSlide : rangeSlides) {
            Duration range = rangeSlide.getFirst();
            Duration slide = rangeSlide.getSecond();
            List<Long> memoryResults = new ArrayList<>();
            AbstractSlidingWindowConnectivity slidingWindowConnectivity = getSwc(method, range, slide, workload, graph, streamingEdges.get(0).timeStamp);
            slidingWindowConnectivity.computeQueriesAndGetMemoryConsumption(
                    streamingEdges,
                    initializeOutput(workload.size()),
                    memoryResults
            );
            // 结果写入到文件
            writePerWindowResult(
                    memoryResults,
                    BENCHMARK_RESULTS + "memory-" + expType + "-" + rangeSlide + "-" + graph + "-" + method + "-" + "workload" + workload.size() + ".txt"
            );
            // 打印
            System.out.println("memory-" + expType + "-" + rangeSlide + "-" + graph + "-" + method + "-" + "workload" + workload.size());
            // 打印结果
            for (Long l : memoryResults)
                System.out.println(l);
            System.gc();
        }
    }

    private static void setupThrExp( // 100 queries
                                     String[] methods,
                                     String expType,
                                     String graph,
                                     List<Pair<Duration, Duration>> rangeAndSlides,
                                     int repeat,
                                     List<String> results) {
        setupThrExp(
                methods,
                expType,
                graph,
                rangeAndSlides,
                repeat,
                results,
                List.of(getWorkLoad(graph, 100)) // List.of(...)方法被用来创建一个只包含一个元素的不可变列表
        );
    }

    private static void setupThrExp( // various sizes of workloads
                                     String[] methods,
                                     String expType,
                                     String graph,
                                     List<Pair<Duration, Duration>> rangeAndSlides,
                                     int repeat,
                                     List<String> results,
                                     List<List<IntIntPair>> workloads) {

        // get graph
        // StreamingEdge类型(source, target, timestamp)
        List<StreamingEdge> streamingEdges = GraphUtils.readStreamingGraph(BENCHMARK_DATASETS + graph + ".txt", ",");

        // 外循环负载(workload) 内循环算法(method)
        for (List<IntIntPair> workload : workloads) { // 遍历 workloads
            System.out.println("Workload size: " + workload.size()); // 打印工作负载（返回负载数量）

            if (rangeAndSlides == null)
                return;

            System.out.println("Range and slide: " + rangeAndSlides); // 打印 range 和 slides

            for (String method : methods) // 遍历 methods（D-Tree, RWC, BIC, ET-Tree, HDT）
                runThrExp(
                        graph,
                        method,
                        expType,
                        rangeAndSlides,
                        repeat,
                        workload,
                        streamingEdges,
                        results
                );
        }
    }

    private static void setupLatencyExp(
            String[] methods,
            String expType,
            String graph,
            List<Pair<Duration, Duration>> rangeAndSlides) { // 100 queries
        setupLatencyExp(
                methods,
                expType,
                graph,
                rangeAndSlides,
                List.of(getWorkLoad(graph, 100))
        );
    }

    private static void setupLatencyExp(
            String[] methods,
            String expType,
            String graph,
            List<Pair<Duration, Duration>> rangeAndSlides,
            List<List<IntIntPair>> workloads) { // various sizes of workloads
        // get graph
        List<StreamingEdge> streamingEdges = GraphUtils.readStreamingGraph(BENCHMARK_DATASETS + graph + ".txt", ",");

        for (List<IntIntPair> workload : workloads) {
            System.out.println("Workload size: " + workload.size());

            if (rangeAndSlides == null)
                return;

            System.out.println("Range and slide: " + rangeAndSlides);

            for (String method : methods) // 遍历 methods（D-Tree, RWC, BIC, ET-Tree, HDT）
                runLatExp(
                        expType,
                        graph,
                        method,
                        rangeAndSlides,
                        workload,
                        streamingEdges
                );
        }
    }

    private static void runThrExp(
            String graph,
            String method,
            String expType,
            List<Pair<Duration, Duration>> rangeSlides,
            int repeat,
            List<IntIntPair> workload,
            List<StreamingEdge> streamingEdges,
            List<String> results) {
        // 打印实验类型 + 方法 + 数据集 + range和slide
        // e.g. Start per-eva throughput experiments for BIC on sg-wiki-topcats with ranges and slides of [(PT10H,PT30M)]
        System.out.println("Start " + expType + " throughput experiments for " + method + " on " + graph + " with ranges and slides of " + rangeSlides);
        
        for (Pair<Duration, Duration> rangeSlide : rangeSlides) {
            Duration range = rangeSlide.getFirst(); // 得到 range
            Duration slide = rangeSlide.getSecond(); // 得到 slide
            for (int i = 0; i < repeat; i++) { // 执行repeat(6)次
                AbstractSlidingWindowConnectivity slidingWindowConnectivity = getSwc(method, range, slide, workload, graph, streamingEdges.get(0).timeStamp); // BenchmarkRunner 第940行
                
                long start = System.nanoTime(); // 开始时间(1秒=10^9纳秒)
                
                slidingWindowConnectivity.computeSlidingWindowConnectivity( // AbstractSlidingWindowConnectivity.java 第32行
                        streamingEdges,
                        initializeOutput(workload.size())
                );
                
                long end = System.nanoTime(); // 结束时间
                
                String result = graph + "," + expType + "," + method + "," + range.toMillis() + "," + slide.toMillis() + "," + streamingEdges.size() + "," + (end - start) + "," + workload.size();
                System.out.println(result); // 打印最终的结果
                // 数据集, 实验类型, 方法, range, slide, 数据集大小(非重复边的数量), 运行时间, 工作负载大小(limit)
                // sg-wiki-topcats,per-eva,BIC,36000000,1800000,25444207,10478799110(10.4788秒),100
                // sg-wiki-topcats,per-eva,BIC,36000000,1800000,25444207,9972964865,100
                // sg-wiki-topcats,per-eva,BIC,36000000,1800000,25444207,9739527155,100
                // sg-wiki-topcats,per-eva,BIC,36000000,1800000,25444207,10124519320,100
                // sg-wiki-topcats,per-eva,BIC,36000000,1800000,25444207,9620980366,100
                // sg-wiki-topcats,per-eva,BIC,36000000,1800000,25444207,9990710165,100
                results.add(result);
                System.gc(); // 用来请求垃圾收集器执行垃圾回收（不是强制执行垃圾回收）
            }
        }
    }

    private static void runLatExp(
            String exp,
            String graph,
            String method,
            List<Pair<Duration, Duration>> rangeAndSlides,
            List<IntIntPair> workload,
            List<StreamingEdge> streamingEdges) {

        System.out.println("Warmup");
        getSwc(method, rangeAndSlides.get(0).getFirst(), rangeAndSlides.get(0).getSecond(), workload, graph, streamingEdges.get(0).timeStamp).computeSlidingWindowConnectivity(
                streamingEdges,
                initializeOutput(workload.size())
        );// warm up

        System.out.println("Start latency " + exp + " experiments for " + method + " on " + graph + " with ranges and slides: " + rangeAndSlides);

        for (Pair<Duration, Duration> pair : rangeAndSlides) {
            Duration range = pair.getFirst(), slide = pair.getSecond();
            AbstractSlidingWindowConnectivity slidingWindowConnectivity = getSwc(method, range, slide, workload, graph, streamingEdges.get(0).timeStamp);
            List<Long> result = new ArrayList<>();
            slidingWindowConnectivity.computeSlidingWindowConnectivity( // AbstractSlidingWindowConnectivity.java 第70行
                    streamingEdges,
                    initializeOutput(workload.size()),
                    result);

            writePerWindowResult(result, BENCHMARK_RESULTS + "latency-" + exp + "-" + pair + "-" + graph + "-" + method + "-" + "workload" + workload.size() + "-" + LocalDateTime.now() + ".txt");
            System.gc();
        }
    }

    private static AbstractSlidingWindowConnectivity getSwc(String method, Duration range, Duration slide, List<IntIntPair> workload, String graph, long first) {
        AbstractSlidingWindowConnectivity ret;
        switch (method) {
            case "DFS":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new DfsConnectivity());
                break;
            case "RWC":
                ret = new RecalculatingWindowConnectivity(range, slide, workload);
                break;
            case "ET-Tree":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new EtrConnectivity());
                break;
            case "HDT":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new HdtConnectivity(GRAPH_VERTEX_NUM.get(graph))); // hdt needs the number of vertices to initialize the number of layers
                break;
            case "D-Tree":
                ret = new FdcSlidingWindowConnectivity(range, slide, workload, new DTreeConnectivity());
                break;
            case "BIC":
                ret = new BidirectionalIncrementalConnectivity(range, slide, first, workload); // BIC uses first timestamp to initialize chunk
                break;
            default:
                ret = null;
        }
        return ret;
    }

    private static void writeResult(List<String> results, String path) {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(path);
            fileWriter.append("graph,experiment,method,range,slide,size,time,workload").append("\n").flush();
            for (String record : results)
                fileWriter.append(record).append("\n").flush();
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 将result列表中的每个元素写入到指定路径path的文件中，每个元素占一行
    private static void writePerWindowResult(List<Long> result, String path) {
        FileWriter fileWriter; // 写入字符到文件的便捷类
        try {
            fileWriter = new FileWriter(path); // 新的文件输出流会被创建(如果文件已存在则会被覆盖)
            for (Long record : result)
                fileWriter.append(record.toString()).append("\n"); // 每个记录被转换为字符串(通过调用toString()方法)
            fileWriter.flush(); // 确保所有缓冲的输出数据都被写入到文件中
            fileWriter.close(); // 关闭文件输出流，释放与之相关的资源
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<IntIntPair> getFullWorkLoad(String graph) {
        return WorkloadUtils.readWorkload(BENCHMARK_WORKLOADS + graph + ".json"); // ./benchmark/workloads/sg-wiki-topcats.json
    }

    private static List<IntIntPair> getWorkLoad(String graph, int limit) {
        List<IntIntPair> temp = getFullWorkLoad(graph); // 得到全部负载（返回一个IntIntPair对象的列表，并将其存储在局部变量temp中）
        List<IntIntPair> workload = new ArrayList<>();
        Random random = new Random(1700276688);
        for (int i = 0; i < limit; i++) // test a workload of 100 queries
            workload.add(temp.get(random.nextInt(temp.size()))); // 从temp列表中随机选择limit个IntIntPair对象添加到workload列表中
        return workload; // 返回最终的workload列表
    }

    private static List<List<IntIntPair>> getWorkLoads(String graph, int[] sizes) { // sizes: {1, 10, 100, 1000, 10000}
        List<List<IntIntPair>> ret = new ArrayList<>();
        for (int size : sizes)
            ret.add(getWorkLoad(graph, size));
        return ret;
    }

    static List<List<Boolean>> initializeOutput(int num) { // 根据负载数量(workload size: 100)，初始化输出大小
        List<List<Boolean>> ret = new ArrayList<>();
        for (int i = 0; i < num; i++)
            ret.add(new ArrayList<>());
        return ret;
    }
}
