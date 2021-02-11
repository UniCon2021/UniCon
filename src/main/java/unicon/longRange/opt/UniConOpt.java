package unicon.longRange.opt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class UniConOpt extends Configured implements Tool {
    private Logger logger = Logger.getLogger(getClass());

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UniConOpt(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        String inputString = args[0];
        String outputString = args[1];

        int numPartitions = conf.getInt("numPartitions", 1);
        int threshold = conf.getInt("threshold", 0);
        long maxNode = conf.getLong("numNodes", -1);
        conf.setInt("numPartitions", numPartitions);
        conf.setInt("threshold", threshold);
        conf.setLong("maxNode", maxNode);

        FileSystem fs = FileSystem.get(conf);

        logger.info("Input                : " + inputString);
        logger.info("Output               : " + outputString);
        logger.info("Number of partitions : " + numPartitions);
        logger.info("threshold            : " + threshold);
        logger.info("Number of nodes      : " + maxNode);

        long time = System.currentTimeMillis();
        long totalTime = time;

        Path input;
        Path output;
        boolean converge = false;
        int round = 0;
        long numChanges;

        input = new Path(inputString);
        output = input.suffix(".0/out");
        fs.delete(output, true);

        InitByUnionFindWithLocalization init = new InitByUnionFindWithLocalization(input, output, true);
        ToolRunner.run(conf, init, args);

        logger.info(String.format("Round 0 (UniCon-opt init) ends :\t%.2fs", ((System.currentTimeMillis() - time) / 1000.0)));

        long edgeSize = init.outputSize;
        long inSize, ccSize, removedSize;

        UniStarOpt uniStarOpt;

        while (!converge) {
            logger.info(String.format("(UniCon-opt) Round %d #input edges: %d", round+1, edgeSize));

            if (edgeSize > threshold) {
                input = new Path(inputString + "." + round + "/out");
                round++;
                output = new Path(inputString + "." + round);
                uniStarOpt = new UniStarOpt(input, output);
                time = System.currentTimeMillis();
                ToolRunner.run(conf, uniStarOpt, args);
                numChanges = (int) uniStarOpt.numChanges;
                if (numChanges == 0) converge = true;
                edgeSize = uniStarOpt.numEdges;
                inSize = uniStarOpt.numInEdges;
                ccSize = uniStarOpt.numCcEdges;
                removedSize = uniStarOpt.numRemovedEdges;
                logger.info(String.format("Round %d (UniStar-opt) ends :\t#IN file size(%d), #CC file size(%d), #REMOVED edge(%d), #OUT file size(%d)", round, inSize, ccSize, removedSize, edgeSize));
                logger.info(String.format("Round %d (UniStar-opt) ends :\tchange(%d)\t%.2fs",
                        round, numChanges,
                        ((System.currentTimeMillis() - time) / 1000.0)));
            }
            else {
                RemUFJob lcc = new RemUFJob(new Path(inputString + "." + round + "/out"), new Path(inputString + "." + (round + 1) + "/out"), maxNode);

                time = System.currentTimeMillis();
                ToolRunner.run(conf, lcc, null);

                round++;
                logger.info(String.format("Round %d (local) ends :\tout(%d)\t%.2fs",
                        round, lcc.outputSize, ((System.currentTimeMillis() - time) / 1000.0)));

                edgeSize = lcc.outputSize;
                converge = true;
            }
        }

        time = System.currentTimeMillis();

        input = new Path(inputString);
        output = new Path(outputString);
        fs.delete(output, true);
        ToolRunner.run(conf, new CCComputationOpt(input, output, round), args);

        logger.info(String.format("Round %d (CCComputation) ends :\t%.2fs",
                ++round, ((System.currentTimeMillis() - time) / 1000.0)));

        for(int r = 0; r < round; r++){
            fs.delete(new Path(inputString + "." + r), true);
        }

        System.out.print("[UniCon-opt-end]\t" + new Path(inputString).getName() + "\t" + new Path(outputString).getName() + "\t" + numPartitions + "\t" + round + "\t");
        System.out.print( ((System.currentTimeMillis() - totalTime)/1000.0) + "\t" );
        System.out.println("# input output numPartitions numRounds time(sec)");

        return 0;
    }
}
