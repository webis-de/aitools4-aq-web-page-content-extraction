package de.aitools.aq.web.extractor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cmu.lemurproject.WarcFileInputFormat;
import edu.cmu.lemurproject.WarcHTMLResponseRecord;
import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

/**
 * Class that runs an {@link HtmlSentenceExtractor} on Hadoop.
 *
 * <p>
 * If you want to write a new extractor, you don't have to care about this
 * class, as the {@link HtmlSentenceExtractor} base class does the interfacing
 * for you.
 * </p><p>
 * When an extraction fails, this is just recorded in the counters of the job,
 * but the mappers will continue and ignore this particular WARC record.
 * </p><p>
 * Currently, this only supports reading WARCs. Each mapper will write all
 * extracted sentences line-by-line to an own gzipped file in the output
 * directory.
 * </p>
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date: 2016/11/17 16:36:00 $
 *
 */
public class HadoopHtmlSentenceExtractionTool implements Tool {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  private static final String PARAM_ARGS = "extraction.args";
  
  private static final String PARAM_EXTRACTOR = "extraction.extractor";

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  private Configuration configuration;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Creates a new tool.
   */
  public HadoopHtmlSentenceExtractionTool() {
    this.configuration = null;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  @Override
  public void setConf(final Configuration configuration) {
    this.configuration = configuration;
  }
  
  /**
   * Adds the extractor class information to the given configuration.
   * <p>
   * This configuration can then be passed to
   * {@link ToolRunner#run(Configuration, Tool, String[])}.
   * </p>
   * @param configuration The configuration to change
   * @param extractorClass The class of the extractor to use
   */
  public static void configure(
      final Configuration configuration,
      final Class<? extends HtmlSentenceExtractor> extractorClass) {
    configuration.set(PARAM_EXTRACTOR, extractorClass.getName());
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  public int run(final String[] args) throws Exception {
    final Configuration configuration = this.getConf();
    configuration.setStrings(PARAM_ARGS, args);

    @SuppressWarnings("unchecked")
    final Class<? extends HtmlSentenceExtractor> extractorClass =
        (Class<? extends HtmlSentenceExtractor>)
          Class.forName(configuration.get(PARAM_EXTRACTOR));
    final HtmlSentenceExtractor extractor = extractorClass.newInstance();
    final Job job = Job.getInstance(configuration, extractorClass.getName());
    
    final Options options = extractor.getOptions();
    final CommandLineParser parser = new GnuParser();
    final CommandLine config = parser.parse(options, args);
    
    job.setJobName(extractorClass.getName() + " " + Arrays.toString(args));
    job.setJarByClass(extractorClass);
    job.setMapperClass(WarcMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(WarcFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    TextOutputFormat.setCompressOutput(job, true);
    TextOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    

    final String[] inputFileNames =
        config.getOptionValues(HtmlSentenceExtractor.FLAG_INPUT);
    for (final String inputFileName : inputFileNames) {
      FileInputFormat.addInputPath(job, new Path(inputFileName));
    }
    
    final String outputFileName =
        config.getOptionValue(HtmlSentenceExtractor.FLAG_OUTPUT);
    FileOutputFormat.setOutputPath(job, new Path(outputFileName));

    // Run it
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * Mapper to extract sentences from WARC files.
   *
   * @author johannes.kiesel@uni-weimar.de
   * @version $Date: 2016/11/17 16:36:00 $
   *
   */
  public static class WarcMapper
  extends Mapper<LongWritable, WritableWarcRecord, Text, Text> {

    private static final Text EMPTY_TEXT = new Text("");

    public static enum COUNTERS {
      VALID_FILES,
      VALID_ZERO_SENTENCE_FILES,
      EXTRACTION_ERRORS,
      EXTRACTION_TIMEOUT_ERRORS,
      OUTPUT_NUM_SENTENCES,
    }
    
    private HtmlSentenceExtractor extractor;
    
    private boolean writeNames;
    
    public WarcMapper() {
      this.extractor = null;
      this.writeNames = false;
    }
    
    @Override
    protected void setup(final Context context) {
      final Configuration configuration = context.getConfiguration();
      
      try {
        @SuppressWarnings("unchecked")
        final Class<? extends HtmlSentenceExtractor> extractorClass =
            (Class<? extends HtmlSentenceExtractor>)
              Class.forName(configuration.get(PARAM_EXTRACTOR));
        this.extractor = extractorClass.newInstance();
      } catch (final InstantiationException | IllegalAccessException
          | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      

      final Options options = this.extractor.getOptions();
      final String[] args = configuration.getStrings(PARAM_ARGS);
      final CommandLineParser parser = new GnuParser();
      try {
        final CommandLine config = parser.parse(options, args);
        this.writeNames =
            config.hasOption(HtmlSentenceExtractor.FLAG_WRITE_NAMES);
        this.extractor.configure(config);
      } catch (final ParseException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void map(final LongWritable key, final WritableWarcRecord value,
        final Context context)
    throws IOException, InterruptedException {
      final WarcRecord warcRecord = value.getRecord();
      List<String> sentences = null;
      try {
        final String html = Warcs.getHtml(warcRecord);
        sentences = this.extractor.extractSentences(html);
      } catch (final Throwable e) {
        final Throwable cause = e.getCause();
        if (cause != null && cause instanceof TimeoutException) {
          context.getCounter(COUNTERS.EXTRACTION_TIMEOUT_ERRORS).increment(1);
        }
        context.getCounter(COUNTERS.EXTRACTION_ERRORS).increment(1);
      }

      if (sentences != null) {
        context.getCounter(COUNTERS.VALID_FILES).increment(1);

        if (sentences.isEmpty()) {
          context.getCounter(COUNTERS.VALID_ZERO_SENTENCE_FILES).increment(1);
        } else {
          final WarcHTMLResponseRecord htmlWarcRecord =
              new WarcHTMLResponseRecord(warcRecord);

          if (this.writeNames) {
            context.write(EMPTY_TEXT, EMPTY_TEXT);
            context.write(EMPTY_TEXT, EMPTY_TEXT);
            final StringBuilder names = new StringBuilder();
            final String uri = htmlWarcRecord.getTargetURI();
            if (uri != null) { names.append(uri); }
            names.append(' ');
            final String trecId = htmlWarcRecord.getTargetTrecID();
            if (trecId != null) { names.append(trecId); }
            this.writeSentence(names.toString(), context);
          }

          for (final String sentence : sentences) {
            this.writeSentence(sentence, context);
          }
          context.getCounter(COUNTERS.OUTPUT_NUM_SENTENCES).increment(
              sentences.size());
        }
      }
      context.progress();
    }

    protected void writeSentence(final String sentence, final Context context)
    throws IOException, InterruptedException {
      final Text text = new Text(sentence);
      context.write(text, EMPTY_TEXT);
    }
    
  }
  

}
