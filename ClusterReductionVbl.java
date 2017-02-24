
// All imports here
import java.io.IOException;
import edu.rit.io.InStream;
import edu.rit.io.OutStream;
import edu.rit.pj2.Tuple;
import edu.rit.pj2.Vbl;

/**
 * Reduction Vbl class which extends Streamable Tuple and implements Vbl for
 * reduction
 * 
 * @author Pavan Prabhakar Bhat (pxb8715@rit.edu)
 *
 */
public class ClusterReductionVbl extends Tuple implements Vbl {

	// Current index of the matching DNA Query
	long index;
	// Score for the current index of the matching DNA Query
	long score;

	// Constructor for reduction variable
	public ClusterReductionVbl() {
		super();
	}

	// Parameterized Constructor for reduction variable
	public ClusterReductionVbl(long index, long score) {
		this.index = index;
		this.score = score;
	}

	/**
	 * This function creates a clone of the global value for index, score and
	 * threshold variable which is to be provided to the local threads.
	 * 
	 */
	public Object clone() {

		ClusterReductionVbl vbl = (ClusterReductionVbl) super.clone();
		vbl.index = this.index;
		vbl.score = this.score;
		return vbl;
	}

	/**
	 * Reduces the count of score, index, and threshold from local to global
	 * 
	 */
	@Override
	public void reduce(Vbl vbl) {
		// Checks the score and sorts it if scores are equal then checks the
		// index and adds the id with the lowest index
		if (this.score < ((ClusterReductionVbl) vbl).score) {
			this.index = ((ClusterReductionVbl) vbl).index;
			this.score = ((ClusterReductionVbl) vbl).score;

		} else if (this.score == ((ClusterReductionVbl) vbl).score) {
			if (this.index > ((ClusterReductionVbl) vbl).index) {
				this.index = ((ClusterReductionVbl) vbl).index;
				this.score = ((ClusterReductionVbl) vbl).score;

			}
		}
	}

	/**
	 * Sets the reduced value to the count of index, score and threshold
	 * 
	 */
	@Override
	public void set(Vbl vbl) {
		this.index = ((ClusterReductionVbl) vbl).index;
		this.score = ((ClusterReductionVbl) vbl).score;

	}

	/**
	 * Writes the tuples into the tuple space
	 *
	 */
	@Override
	public void writeOut(OutStream out) throws IOException {
		out.writeLong(this.index);
		out.writeLong(this.score);
	}

	/**
	 * Reads the tuples from the tuple space
	 * 
	 */
	@Override
	public void readIn(InStream in) throws IOException {
		this.index = in.readLong();
		this.score = in.readLong();
	}

}
