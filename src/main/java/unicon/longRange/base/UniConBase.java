package unicon.longRange.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class UniConBase extends Configured implements Tool {
    private Logger logger = Logger.getLogger(getClass());

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new UniConBase(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        String inputString = args[0];
        String outputString = args[1];

        int numPartitions = conf.getInt("numPartitions", 1);
        conf.setInt("numPartitions", numPartitions);
        long maxNode = conf.getLong("numNodes", -1);
        conf.setLong("maxNode", maxNode);

        FileSystem fs = FileSystem.get(conf);

        logger.info("Input                : " + inputString);
        logger.info("Output               : " + outputString);
        logger.info("Number of partitions : " + numPartitions);
        logger.info("Number of nodes      : " + maxNode);

        long time = System.currentTimeMillis();
        long totalTime = time;

        Path input;
        Path output;
        boolean converge = false;
        int round = 0;
        long numChanges;

        input = new Path(inputString);
        output = input.suffix("." + round);

        InitByUnionFindWithLocalization init = new InitByUnionFindWithLocalization(input, output, true);
        ToolRunner.run(conf, init, args);

        logger.info(String.format("Round 0 (Egiza init) ends :\t%.2fs", ((System.currentTimeMillis() - time) / 1000.0)));

        long edgeSize = init.outputSize;

        UniStar uniStar;

        while (!converge) {
            logger.info(String.format("(Egiza) Round %d #input edges: %d", round+1, edgeSize));

            input = new Path(inputString + "." + round);
            round++;
            output = new Path(inputString + "." + round);
            uniStar = new UniStar(input, output);
            time = System.currentTimeMillis();
            ToolRunner.run(conf, uniStar, args);
            numChanges = uniStar.numChanges;
            if (numChanges == 0) converge = true;

            logger.info(String.format("Round %d (EgizaUF) ends :\t#input edges of reducer(%d)\tchange(%d)\t%.2fs",
                    round, uniStar.reducerInputSize, uniStar.numChanges,
                    ((System.currentTimeMillis() - time) / 1000.0)));

        }

        input = new Path(inputString + "." + round);

        time = System.currentTimeMillis();
        ToolRunner.run(conf, new CCComputation(input, new Path(outputString)), args);

        round++;
        logger.info(String.format("Round %d (CCComputation) ends :\t%.2fs",
                round, ((System.currentTimeMillis() - time) / 1000.0)));

        for(int r = 0; r < round; r++){
            fs.delete(new Path(inputString + "." + r), true);
        }

        System.out.print("[EgizaBase-end]\t" + new Path(inputString).getName() + "\t" + new Path(outputString).getName() + "\t" + numPartitions + "\t" + round + "\t");
        System.out.print( ((System.currentTimeMillis() - totalTime)/1000.0) + "\t" );
        System.out.println("# input output numPartitions numRounds time(sec)");

        return 0;
    }
}
