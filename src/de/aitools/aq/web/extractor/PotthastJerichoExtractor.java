package de.aitools.aq.web.extractor;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import de.aitools.aq.text.StopWordFilter;
import de.aitools.aq.text.TextFilter;
import de.aitools.aq.text.WordFilter;
import de.aitools.aq.text.WordMatchFilter;

/**
 * A sentence extractor that uses a few heuristics to filter undesired
 * paragraphs and sentences. Based on an idea by Martin Potthast.
 * 
 * <p>
 * The extractor discard too small paragraphs, sentences with too few function
 * words (also known as stop words), and sentences with too few proper words
 * (naively defined as tokens that only consist of alphabetic characters and
 * hyphens within). 
 * </p><p>
 * The default settings are the ones used in
 * <pre>
 * Johannes Kiesel, Benno Stein, and Stefan Lucks.
 * A Large-scale Analysis of the Mnemonic Password Advice.
 * In Proceedings of the 24th Annual Network and Distributed System Security Symposium (NDSS 17),
 * February 2017. 
 * </pre>
 * The settings were found to be the best for extracting complete main content
 * sentences while favoring precision over recall.
 * </p>
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date: 2016/11/21 22:10:45 $
 *
 */
public class PotthastJerichoExtractor extends JerichoHtmlSentenceExtractor {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  private static String SHORT_FLAG_MIN_PARAGRAPH_LENGTH = "l";
  
  private static String FLAG_MIN_PARAGRAPH_LENGTH = "min-paragraph-length";

  private static String SHORT_FLAG_MIN_STOP_WORDS = "sw";
  
  private static String FLAG_MIN_STOP_WORDS = "min-stop-words";

  private static String SHORT_FLAG_MIN_STOP_WORD_RATIO = "swr";
  
  private static String FLAG_MIN_STOP_WORD_RATIO = "min-stop-word-ratio";

  private static String SHORT_FLAG_MIN_MATCHING_WORDS = "mw";
  
  private static String FLAG_MIN_MATCHING_WORDS = "min-matching-words";

  private static String SHORT_FLAG_MIN_MATCHING_WORD_RATIO = "mwr";
  
  private static String FLAG_MIN_MATCHING_WORD_RATIO = "min-matching-word-ratio";
  
  

  public static final String DEFAULT_MATCHING_WORD_PATTERN =
      "^\\p{IsAlphabetic}[-\\p{IsAlphabetic}]*\\p{IsAlphabetic}*$";

  public static final int DEFAULT_MIN_PARAGRAPH_LENGTH = 400;

  public static final int DEFAULT_MIN_NUM_STOP_WORDS_IN_SENTENCE = 1;

  public static final double DEFAULT_MIN_MATCHING_WORD_RATIO = 0.5;

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////
  
  private int minParagraphLengthInCharacters;
  
  private StopWordFilter stopWordFilter;
  
  private WordMatchFilter wordMatchFilter;

  private final TextFilter stopWordTextFilter;
  
  private final TextFilter wordMatchTextFilter;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a new extractor with default settings. Note that the default
   * settings include to only extract English text.
   */
  public PotthastJerichoExtractor() {
    this.setMinParagraphLengthInCharacters(DEFAULT_MIN_PARAGRAPH_LENGTH);
    this.stopWordFilter = new StopWordFilter(true);
    this.wordMatchFilter = new WordMatchFilter(DEFAULT_MATCHING_WORD_PATTERN);
    this.stopWordTextFilter = new TextFilter(this.stopWordFilter);
    this.setMinStopWordsInSentence(DEFAULT_MIN_NUM_STOP_WORDS_IN_SENTENCE);
    this.wordMatchTextFilter = new TextFilter(this.wordMatchFilter);
    this.setMinMatchingWordRatioInSentence(DEFAULT_MIN_MATCHING_WORD_RATIO);
    this.setExtractLanguage(Locale.ENGLISH);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public void setExtractLanguages(final Collection<Locale> targetLanguages) {
    super.setExtractLanguages(targetLanguages);
    if (this.stopWordFilter != null) {
      this.stopWordFilter.retainStopWordLists(targetLanguages);
    }
  }
  
  /**
   * Sets the size threshold for paragraphs (in number of characters) to not be
   * discarded.
   */
  public void setMinParagraphLengthInCharacters(
      final int minParagraphLengthInCharacters) {
    this.minParagraphLengthInCharacters = minParagraphLengthInCharacters;
  }
  
  /**
   * Sets the minimum number of stop words a sentence must contain to not be
   * discarded. Stop word lists are used based on the detected paragraph
   * language.
   */
  public void setMinStopWordsInSentence(
      final int minStopWordsInSentence) {
    this.stopWordTextFilter.setMinAbsolute(minStopWordsInSentence);
  }

  /**
   * Sets the minimum ratio of stop words relative to all words a sentence must
   * contain to not be discarded. Stop word lists are used based on the detected
   * paragraph language.
   */
  public void setMinStopWordRatioInSentence(
      final double minStopWordRatioInSentence) {
    this.stopWordTextFilter.setMinRatio(minStopWordRatioInSentence);
  }
  
  /**
   * Sets the minimum number of matching words a sentence must contain to not be
   * discarded. Matching words are defined by the {@link WordMatchFilter} (see
   * {@link #setMatchingWordFilter(WordMatchFilter)}), and are by default words
   * that contain only alphabetic characters, possibly separated by hyphens.
   */
  public void setMinMatchingWordsInSentence(
      final int minMatchingWordsInSentence) {
    this.wordMatchTextFilter.setMinAbsolute(minMatchingWordsInSentence);
  }

  /**
   * Sets the minimum ratio of matching words relative to all words a sentence
   * must contain to not be discarded. Matching words are defined by the
   * {@link WordMatchFilter} (see
   * {@link #setMatchingWordFilter(WordMatchFilter)}), and are by default words
   * that contain only alphabetic characters, possibly separated by hyphens.
   */
  public void setMinMatchingWordRatioInSentence(
      final double minMatchingWordRatioInSentence) {
    this.wordMatchTextFilter.setMinRatio(minMatchingWordRatioInSentence);
  }
  
  /**
   * Sets the stop word filter to be used to determine stop words in a sentence.
   */
  public void setStopWordFilter(final StopWordFilter stopWordFilter) {
    this.stopWordTextFilter.setWordFilter(stopWordFilter);
    this.stopWordFilter = stopWordFilter;
  }

  /**
   * Sets the word filter to be used to determine "matching" words in a
   * sentence.
   */
  public void setMatchingWordFilter(final WordMatchFilter wordMatchFilter) {
    this.wordMatchTextFilter.setWordFilter(wordMatchFilter);
    this.wordMatchFilter = wordMatchFilter;
  }
  
  @Override
  public void configure(final CommandLine config) {
    super.configure(config);
    
    final String minParagraphLength =
        config.getOptionValue(FLAG_MIN_PARAGRAPH_LENGTH);
    if (minParagraphLength != null) {
      this.setMinParagraphLengthInCharacters(
          Integer.parseInt(minParagraphLength));
    }
    
    final String minStopWords =
        config.getOptionValue(FLAG_MIN_STOP_WORDS);
    if (minStopWords != null) {
      this.setMinStopWordsInSentence(
          Integer.parseInt(minStopWords));
    }
    final String minStopWordRatio =
        config.getOptionValue(FLAG_MIN_STOP_WORD_RATIO);
    if (minStopWordRatio != null) {
      this.setMinStopWordRatioInSentence(
          Double.parseDouble(minStopWordRatio));
    }
    
    final String minMatchingWords =
        config.getOptionValue(FLAG_MIN_MATCHING_WORDS);
    if (minMatchingWords != null) {
      this.setMinMatchingWordsInSentence(
          Integer.parseInt(minMatchingWords));
    }
    final String minMatchingWordRatio =
        config.getOptionValue(FLAG_MIN_MATCHING_WORD_RATIO);
    if (minMatchingWordRatio != null) {
      this.setMinMatchingWordRatioInSentence(
          Double.parseDouble(minMatchingWordRatio));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  protected boolean isValidParagraph(
      final String paragraph, final Locale paragraphLanguage) {
    return paragraph.length() >= this.minParagraphLengthInCharacters;
  }
  
  @Override
  protected boolean isValidSentence(
      final String sentence, final Locale paragraphLanguage) {
    final List<String> words = WordFilter.toWords(sentence, paragraphLanguage);
    
    return this.stopWordTextFilter.test(words, paragraphLanguage)
        && this.wordMatchTextFilter.test(words, paragraphLanguage);
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   PROGRAM                                //
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public Options addOptions(final Options options) {
    super.addOptions(options);
    
    final Option minParagraphLengthOption = new Option(
        SHORT_FLAG_MIN_PARAGRAPH_LENGTH, true,
        "Sets the size threshold for paragraphs (in number of characters) to "
        + "not be discarded (Current: " + this.minParagraphLengthInCharacters
        + ")");
    minParagraphLengthOption.setLongOpt(FLAG_MIN_PARAGRAPH_LENGTH);
    minParagraphLengthOption.setArgName("min");
    options.addOption(minParagraphLengthOption);
    
    final Option minStopWordsOption = new Option(
        SHORT_FLAG_MIN_STOP_WORDS, true,
        "Sets the minimum number of stop words a sentence must contain to not "
        + "be discarded. Stop word lists are used based on the detected "
        + "paragraph language (Current: "
        + this.stopWordTextFilter.getMinAbsolute() + ")");
    minStopWordsOption.setLongOpt(FLAG_MIN_STOP_WORDS);
    minStopWordsOption.setArgName("min");
    options.addOption(minStopWordsOption);
    
    final Option minStopWordRatioOption = new Option(
        SHORT_FLAG_MIN_STOP_WORD_RATIO, true,
        "Sets the minimum ratio of stop words relative to all words a sentence "
        + "must contain to not be discarded (Current: "
        + this.stopWordTextFilter.getMinRatio() + ")");
    minStopWordRatioOption.setLongOpt(FLAG_MIN_STOP_WORD_RATIO);
    minStopWordRatioOption.setArgName("min");
    options.addOption(minStopWordRatioOption);
    
    final Option minMatchingWordsOption = new Option(
        SHORT_FLAG_MIN_MATCHING_WORDS, true,
        "Sets the minimum number of matching words a sentence must contain to "
        + "not be discarded. Matching words contain only alphabetic "
        + "characters, possibly separated by hyphens.");
    minMatchingWordsOption.setLongOpt(FLAG_MIN_MATCHING_WORDS);
    minMatchingWordsOption.setArgName("min");
    options.addOption(minMatchingWordsOption);
    
    final Option minMatchingWordRatioOption = new Option(
        SHORT_FLAG_MIN_MATCHING_WORD_RATIO, true,
        "Sets the minimum ratio of matching words relative to all words a "
        + "sentence must contain to not be discarded.");
    minMatchingWordRatioOption.setLongOpt(FLAG_MIN_MATCHING_WORD_RATIO);
    minMatchingWordRatioOption.setArgName("min");
    options.addOption(minMatchingWordRatioOption);
    
    return options;
  }

  public static void main(final String[] args) throws Exception {
    HtmlSentenceExtractor.main(args, PotthastJerichoExtractor.class);
  }

}
