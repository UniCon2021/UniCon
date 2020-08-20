package unicon.longRange.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class UniConBaseTest {

    @Test
    public void testOnDataset_PT() throws Exception {
        doTest("src/test/resources/datasets/pt.p", 3774768, 10);
    }

    @Test
    public void testOnDataset_FB() throws Exception {
        doTest("src/test/resources/datasets/fb.tsv", 4039, 10);
    }

    @Test
    public void testOnDataset_SK() throws Exception {
        doTest("src/test/resources/datasets/SK.p", 1696415, 10);
    }

    @Test
    public void testOnDataset_R0() throws Exception {
        doTest("src/test/resources/datasets/RO_edges", 41773, 5);
    }

    @Test
    public void testOnToyGraphs() throws Exception {
        doTest("src/test/resources/toyGraphs/vline_largenumber", 6000000010L, 280);
    }

    private void doTest(String input, long numNodes, int numPartitions) throws Exception, IOException {
        runUniConBase(input, numNodes, numPartitions);

        ToolRunner.run(new UnionFindJobForTest(numNodes), new String[]{input, "src/test/resources/out_uf"});

        List<Long> result_cnccopt = getResult("src/test/resources/out/part-r-00000");
        List<Long> result_uf = getResult("src/test/resources/out_uf");

        result_cnccopt.sort(null);
        result_uf.sort(null);
        Assert.assertEquals(result_uf, result_cnccopt);
    }

    public void runUniConBase(String input, long numNodes, int numPartitions) throws Exception {
        Configuration config = new Configuration();
        config.setInt("numPartitions", numPartitions);
        config.setLong("numNodes", numNodes);
        ToolRunner.run(config, new UniConBase(), new String[]{input, "src/test/resources/out"});
    }

    private static List<Long> getResult(String inputPath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(inputPath));

        List<Long> result_unicon = new ArrayList<>();

        String line;
        while ((line = br.readLine()) != null) {
            StringTokenizer st = new StringTokenizer(line);

            long u = Long.parseLong(st.nextToken());
            long v = Long.parseLong(st.nextToken());

            long x = u < v ? ((long) u) << 32 | v : ((long) v) << 32 | u;

            result_unicon.add(x);
        }
        br.close();

        return result_unicon;
    }
}
