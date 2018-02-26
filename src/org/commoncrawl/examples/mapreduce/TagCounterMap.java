package org.commoncrawl.examples.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Reducer;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;

public class TagCounterMap {
	private static final Logger LOG = Logger.getLogger(TagCounterMap.class);
	protected static enum MAPPERCOUNTER {
		RECORDS_IN,
		EXCEPTIONS
	}
	
	public static class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	protected static class TagCounterMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {
		private Text outKey = new Text();
		private LongWritable outVal = new LongWritable(1);
		// The HTML regular expression is case insensitive (?i), avoids closing tags (?!/),
		// tries to find just the tag name before any spaces, and then consumes any other attributes.
		private static final String EMAIL_TAG_PATTERN = "[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,6}";
		//private static final String DOMAIN_TAG_PATTERN = "([a-z0-9]+(-[a-z0-9]+)*\\.)+[a-z]{2,}";
		private Pattern patternTag;
		private Matcher matcherTag;
		//private Pattern dPatTag;
		//private Matcher dMatTag;
		private HashMap<String, ArrayList<String>> map = new HashMap<String, ArrayList<String>>();

		@Override
		public void map(Text key, ArchiveReader value, Context context) throws IOException {
			// Compile the regular expression once as it will be used continuously
			patternTag = Pattern.compile(EMAIL_TAG_PATTERN);
			//dPatTag = Pattern.compile(DOMAIN_TAG_PATTERN);
			
			for (ArchiveRecord r : value) {
				try {
					//LOG.info(r.getHeader().getUrl() + " -- " + r.available());
					// We're only interested in processing the responses, not requests or metadata
					if (r.getHeader().getMimetype().equals("application/http; msgtype=response")) {
						// Convenience function that reads the full message into a raw byte array
						byte[] rawData = IOUtils.toByteArray(r, r.available());
						String content = new String(rawData);
						// The HTTP header gives us valuable information about what was received during the request
						String headerText = content.substring(0, content.indexOf("\r\n\r\n"));
						
						// In our task, we're only interested in text/html, so we can be a little lax
						// TODO: Proper HTTP header parsing + don't trust headers
						if (headerText.contains("Content-Type: text/html")) {
							context.getCounter(MAPPERCOUNTER.RECORDS_IN).increment(1);
							// Only extract the body of the HTTP response when necessary
							// Due to the way strings work in Java, we don't use any more memory than before
							String body = content.substring(content.indexOf("\r\n\r\n") + 4);
							// Process all the matched HTML tags found in the body of the document
							URI uri = new URI(r.getHeader().getUrl());
							String domain = uri.getHost();
							
							matcherTag = patternTag.matcher(body);
							while (matcherTag.find()) {
								String tagName = matcherTag.group(0);
								outKey.set(tagName);
								if(map.containsKey(tagName)){
									ArrayList<String> tmp = new ArrayList<String>();
									tmp = map.get(tagName);
									if(!tmp.contains(domain)){
										tmp.add(domain);
										map.replace(tagName, tmp);
										context.write(outKey, outVal);
									}
								}else{
									ArrayList<String> tmp = new ArrayList<String>();
									tmp.add(domain);
									map.put(tagName, tmp);
									context.write(outKey, outVal);
								}
							}
						}
					}
				}
				catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				}
			}
		}
	}
}
