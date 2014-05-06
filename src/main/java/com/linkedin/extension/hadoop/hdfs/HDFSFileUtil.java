package com.linkedin.extension.hadoop.hdfs;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileUtil {
	/**
	 * Read in a file in HDFS as a single string.
	 * @param filepath
	 * @return
	 * @throws IOException
	 */
	static public String readFileAsString(String filepath) throws IOException {
		FSDataInputStream in = null;
		byte[] b = null;
		try {
            FileSystem fs = FileSystem.get(new Configuration());
			Path path = fs.makeQualified(new Path(filepath));
			in = fs.open(path);
			int size = (int) fs.getFileStatus(path).getLen();

			b = new byte[size];
			in.read(b, 0, size);
		} finally {
			if (in != null)
				in.close();
		}

		return new String(b);
	}
}
