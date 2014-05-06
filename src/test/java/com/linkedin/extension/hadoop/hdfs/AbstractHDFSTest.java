package com.linkedin.extension.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

abstract public class AbstractHDFSTest {
	static private Log LOG = LogFactory.getLog(AbstractHDFSTest.class);
	protected MiniDFSCluster cluster;
	protected Configuration conf; // 
	protected FileSystem mfs; // 
	protected FileContext mfc; // 
	protected static final String ROOT_DIR = "/tmp/";
	protected boolean useFCOption = true;

	public void initJunitModeTest() throws Exception {
		LOG.info("initJunitModeTest");

		conf = new HdfsConfiguration();
		conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 64 * 1024 * 1024); // block size set to default 64 M 

		cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
		cluster.waitActive();

		mfs = cluster.getFileSystem();
		mfc = FileContext.getFileContext();

		Path rootdir = new Path(ROOT_DIR);
		mfs.mkdirs(rootdir);
	}

	public void shutdown() {
		cluster.shutdown();
	}

	protected void writeHDFSFile(String fromFilePath, String toFilePath) throws IOException {
		FSDataOutputStream out = null;
		String priceSchemaStr = readFileAsString(fromFilePath);
		
		try {
			Path path = getFullyQualifiedPath(toFilePath);

			out = useFCOption ? mfc.create(path, EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE))
					: mfs.create(path);
			byte[] b = priceSchemaStr.getBytes();
			out.write(b, 0, b.length);
		} finally {
			if (out != null)
				out.close();
		}
	}

	protected String readHDFSFileAsString(String filePath) throws IOException {
		FSDataInputStream in = null;
		byte[] b = null;
		try {
			Path path = getFullyQualifiedPath(filePath);
			in = openInputStream(path);
			int size = (int) getFileSize(path);

			b = new byte[size];
			in.read(b, 0, size);

		} finally {
			if (in != null)
				in.close();
		}

		return new String(b);
	}

	protected FSDataInputStream openInputStream(Path path) throws IOException {
		FSDataInputStream in = useFCOption ? mfc.open(path) : mfs.open(path);
		return in;
	}

	protected Path getFullyQualifiedPath(String pathString) {
		return useFCOption ? mfc.makeQualified(new Path(ROOT_DIR, pathString))
				: mfs.makeQualified(new Path(ROOT_DIR, pathString));
	}

	protected long getFileSize(Path path) throws IOException {
		return useFCOption ? mfc.getFileStatus(path).getLen() : mfs
				.getFileStatus(path).getLen();
	}

	protected String readFileAsString(String fileName) throws IOException {
		String path = getClass().getResource(fileName).getFile();
		File file = new File(path);
		FileInputStream is = null;
		byte[] b = null;
		try {
			is = new FileInputStream(file);
			b = new byte[(int) file.length()];
			is.read(b, 0, (int) file.length());
		} finally {
			if (is != null)
				is.close();
		}
		return new String(b);
	}
}