
// All imports here
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.rit.pjmr.Combiner;
import edu.rit.pjmr.Customizer;
import edu.rit.pjmr.Mapper;
import edu.rit.pjmr.PjmrJob;
import edu.rit.pjmr.Reducer;
import edu.rit.pjmr.TextDirectorySource;
import edu.rit.pjmr.TextId;

/**
 * This class starts the main parallel java map reduce job.
 * 
 * @author Pavan Prabhakar Bhat (pxb8715@rit.edu)
 *
 */
public class DnaQuery extends PjmrJob<TextId, String, String, ClusterReductionVbl> {

	// Threshold for reporting subject sequences
	double threshold;

	/**
	 * The main function that starts the job and runs the mapper and reducer
	 * tasks.
	 */
	@Override
	public void main(String[] args) throws Exception {

		// Parse command line arguments.
		if (args.length != 4)
			usage();
		try {
			// Consists of several nodes taken as arguments which contains the
			// input
			// files
			String[] nodes = args[0].split(",");

			// Contains the directory where the files can be found.
			String directory = args[1];

			// The pattern that is required to be matched with the DNA Sequence
			String pattern = args[2];

			// Threshold for reporting the subject sequences
			threshold = Double.parseDouble(args[3]);

			// Determine number of mapper threads.
			int NT = Math.max(threads(), 1);

			// Holds the pattern and threshold to be passed to the mapper
			String[] tempargs = { pattern, threshold + "" };

			// Configure mapper tasks.
			for (String node : nodes)
				mapperTask(node).source(new TextDirectorySource(directory)).mapper(NT, MyMapper.class, tempargs);

		} catch (Exception e) {
			usage();
			System.err.println(e.getMessage());
		}

		// Configure reducer task.
		reducerTask().customizer(MyCustomizer.class).reducer(MyReducer.class);

		// Starts the Job
		startJob();
	}

	/**
	 * Prints a usage message and exits.
	 */
	private static void usage() {
		System.err.println("Usage: java pj2 jar= p4.jar " + " DnaQuery [<nodes>] " + " [<directory>] " + "[<pattern>]"
				+ "[<threshold>]");
		terminate(1);
	}

	//
	private static class MyMapper extends Mapper<TextId, String, String, ClusterReductionVbl> {

		// Each DNA Sequence string
		public String seq;

		// old DNA ID
		public String oldDnaId;

		// Reporting threshold for subject sequences
		double thresh;

		// Query pattern
		String queryPattern;

		// Given threshold for reporting
		Double givenThresh;

		// Cluster Reduction Variable
		private static ClusterReductionVbl ONE;

		/**
		 * Starts the mapper process. Generally used for initialization.
		 */
		@Override
		public void start(String[] args, Combiner<String, ClusterReductionVbl> combiner) {
			seq = "";
			oldDnaId = "";
			thresh = 0;
			queryPattern = args[0];
			givenThresh = Double.parseDouble(args[1]);
		}

		/**
		 * Maps the input and outputs to the combiner.
		 */
		@Override
		public void map(TextId id, String contents, Combiner<String, ClusterReductionVbl> combiner) {

			// Regex to extract the pattern of the DNA id
			Pattern dnaID = Pattern.compile("g.*\\|");
			// Matcher checks if the dnaID matches with the contents
			Matcher mat = dnaID.matcher(contents);
			// Checks to see if individual DNA sequences are used for
			// computation of scores
			if (mat.find()) {
				if (seq != "") {
					// Converted DNA sequence and querying sequence to char
					// arrays for ease of operation
					char[] dna = seq.toCharArray();
					char[] query = queryPattern.toCharArray();

					for (long i = 0; i < dna.length - query.length; i++) {
						long s = 0L;
						for (long j = 0; j < query.length; j++) {
							if (dna[(int) (i + j)] == query[(int) j]) {
								s++;
							}
						}

						thresh = (double) s / (double) query.length;
						// Checks the threshold for reporting the DNA sequences
						if (givenThresh <= thresh) {
							ONE = new ClusterReductionVbl(i, s);
							combiner.add(oldDnaId, ONE);
						}

					}

				}
				oldDnaId = mat.group();
				seq = "";

			} else {
				// Adds the contents of a single DNA sequence
				seq += contents;
			}

		}

		/**
		 * Maps the input and outputs to the combiner for the last DNA sequence.
		 */
		@Override
		public void finish(Combiner<String, ClusterReductionVbl> combiner) {
			// Checks to see if individual DNA sequences are used for
			// computation of scores for the last DNA sequence
			if (!oldDnaId.equalsIgnoreCase("")) {
				if (seq != "") {
					// Converted DNA sequence and querying sequence to char
					// arrays for ease of operation
					char[] dna = seq.toCharArray();
					char[] query = queryPattern.toCharArray();

					for (long i = 0; i < dna.length - query.length; i++) {
						long s = 0L;
						for (long j = 0; j < query.length; j++) {
							if (dna[(int) (i + j)] == query[(int) j]) {
								s++;
							}
						}

						thresh = (double) s / (double) query.length;
						// Checks the threshold for reporting the DNA sequences
						if (givenThresh <= thresh) {
							ONE = new ClusterReductionVbl(i, s);
							combiner.add(oldDnaId, ONE);
						}

					}

				}
			}
		}

	}

	// Reducer Task customizer class
	private static class MyCustomizer extends Customizer<String, ClusterReductionVbl> {

		/**
		 * Sorts the DNA Sequence results based on score and then its id
		 */
		public boolean comesBefore(String key_1, ClusterReductionVbl value_1, String key_2,
				ClusterReductionVbl value_2) {
			if (value_1.score > value_2.score)
				return true;
			else if (value_1.score < value_2.score)
				return false;
			else
				return key_1.compareTo(key_2) < 0;
		}
	}

	// Reducer class
	private static class MyReducer extends Reducer<String, ClusterReductionVbl> {

		/**
		 * Prints the final output
		 */
		@Override
		public void reduce(String key, ClusterReductionVbl value) {
			// Displays the result
			System.out.printf("%s\t%s\t%s%n", value.score, value.index, key);
			System.out.flush();
		}

	}

}
