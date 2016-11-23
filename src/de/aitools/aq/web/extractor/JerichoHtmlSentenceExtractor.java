package de.aitools.aq.web.extractor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.ibm.icu.text.BreakIterator;

import de.aitools.ie.languagedetection.LanguageDetector;
import net.htmlparser.jericho.Renderer;
import net.htmlparser.jericho.Segment;
import net.htmlparser.jericho.Source;

/**
 * A basic sentence extractor based on the Jericho extraction library.
 * 
 * <p>
 * This outputs every sentence it finds, but does apply a language filter to
 * filter paragraphs based on the detected language.
 * </p><p>
 * In case you need to know whether sentences are from the same paragraph, you
 * can use {@link #setParagraphSeparator(String)}.
 * </p><p>
 * This class is designed to be extended further. This should be done by
 * overriding the {@link #isValidParagraph(String, Locale)} and
 * {@link #isValidSentence(String, Locale)} checks (both of which always return
 * just <tt>true</tt> for this extractor).
 * </p><p>
 * When extending this extractor, see the documentation of
 * {@link HtmlSentenceExtractor} for information on how to integrate parameters
 * into the command line program.
 * </p>
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date: 2016/11/18 16:12:38 $
 *
 */
public class JerichoHtmlSentenceExtractor extends HtmlSentenceExtractor {

  //////////////////////////////////////////////////////////////////////////////
  //                                  CONSTANTS                               //
  //////////////////////////////////////////////////////////////////////////////
  
  private static String SHORT_FLAG_EXTRACT_ALL_LANGUAGES = "la";
  
  private static String FLAG_EXTRACT_ALL_LANGUAGES = "language-extract-all";
  
  private static String SHORT_FLAG_EXTRACT_LANGUAGES = "le";
  
  private static String FLAG_EXTRACT_LANGUAGES = "language-extract";
  
  private static String SHORT_FLAG_USE_LANGUAGE = "lu";
  
  private static String FLAG_USE_LANGUAGE = "language-use";
  
  private static String SHORT_FLAG_DO_NOT_SEPARATE_PARAGRAPHS = "pn";
  
  private static String FLAG_DO_NOT_SEPARATE_PARAGRAPHS = "separate-paragraphs-not";
  
  private static String SHORT_FLAG_PARAGRAPH_SEPARATOR = "pw";
  
  private static String FLAG_PARAGRAPH_SEPARATOR = "separate-paragraphs-with";

  //////////////////////////////////////////////////////////////////////////////
  //                                   MEMBERS                                //
  //////////////////////////////////////////////////////////////////////////////

  private Set<String> targetLanguages;

  private Function<String, Locale> languageDetector;
  
  private String paragraphSeparator;
  
  private boolean separateParagraphs;

  //////////////////////////////////////////////////////////////////////////////
  //                                CONSTRUCTORS                              //
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Creates a new extractor that only extracts English paragraphs and does not
   * separate the output sentences by the paragraphs they came from. 
   */
  public JerichoHtmlSentenceExtractor() {
    this.setExtractLanguage(Locale.ENGLISH);
    this.setDoNotSeparateParagraphs();
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   GETTER                                 //
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Gets the languages that this extractor will extract, or <tt>null</tt> if
   * all languages are used.
   */
  public Set<String> getTargetLanguages() {
    return Collections.unmodifiableSet(this.targetLanguages);
  }
  
  /**
   * Gets the language detector used by this extractor.
   * <p>
   * If none was specified beforehand by either calling
   * {@link #setLanguageDetector(Function)} or {@link #setUseLanguage(Locale)},
   * This will set the language detector to a default language detector and
   * return it.
   * </p>
   */
  public Function<String, Locale> getLanguageDetector() {
    if (this.languageDetector == null) {
      final LanguageDetector languageDetector = new LanguageDetector();
      this.setLanguageDetector(text -> languageDetector.detect(text));
    }
    return this.languageDetector;
  }
  
  /**
   * Checks whether this extractor adds a specific element to the output list of
   * {@link #extractSentences(String)} before sentences from a new paragraph of
   * the Jericho output (usually signaling elements from a different segment of
   * the web page). The element used is {@link #getParagraphSeparator()} and can
   * be set using {@link #setParagraphSeparator(String)}.
   */
  public boolean separatesParagraphs() {
    return this.separateParagraphs;
  }
  
  /**
   * If {@link #separatesParagraphs()} is <tt>true</tt>, gets the String that is
   * added to the list of extracted sentences before sentences from a new
   * paragraph of the Jericho output (usually signaling elements from a
   * different segment of the web page). If {@link #separatesParagraphs()} is
   * <tt>false</tt>, it will return <tt>null</tt>. However, <tt>null</tt> can
   * also be the actual paragraph separator, so checking the value of
   * {@link #separatesParagraphs()} is needed in this case.
   */
  public String getParagraphSeparator() {
    return this.paragraphSeparator;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                CONFIGURATION                             //
  //////////////////////////////////////////////////////////////////////////////
  
  /**
   * Configures this extractor to extract sentences from paragraphs of all
   * languages.
   * <p>
   * This resets the language detector, so you will have to set it again using
   * {@link #setLanguageDetector(Function)} in case you want to use a different
   * one than the default.
   * </p>
   */
  public void setExtractAllLanguages() {
    this.targetLanguages = null;
    this.languageDetector = null;
  }

  /**
   * Configures this extractor to extract sentences from paragraphs that are
   * classified as having the specified languages. Languages should be ISO
   * codes like "en", "de", and so on.
   * <p>
   * This resets the language detector, so you will have to set it again using
   * {@link #setLanguageDetector(Function)} in case you want to use a different
   * one than the default.
   * </p>
   */
  public final void setExtractLanguages(final String... targetLanguages) {
    final List<Locale> locales = new ArrayList<>(targetLanguages.length);
    for (final String targetLanguage : targetLanguages) {
      locales.add(Locale.forLanguageTag(targetLanguage));
    }
    this.setExtractLanguages(locales);
  }

  /**
   * Configures this extractor to extract sentences from paragraphs that are
   * classified as having the specified languages.
   * <p>
   * This resets the language detector, so you will have to set it again using
   * {@link #setLanguageDetector(Function)} in case you want to use a different
   * one than the default.
   * </p>
   */
  public final void setExtractLanguages(final Locale... targetLanguages) {
    this.setExtractLanguages(Arrays.asList(targetLanguages));
  }

  /**
   * Configures this extractor to extract sentences from paragraphs that are
   * classified as having the specified languages.
   * <p>
   * This resets the language detector, so you will have to set it again using
   * {@link #setLanguageDetector(Function)} in case you want to use a different
   * one than the default.
   * </p>
   */
  public void setExtractLanguages(final Collection<Locale> targetLanguages) {
    this.targetLanguages = new HashSet<>(targetLanguages.size());
    for (final Locale targetLanguage : targetLanguages) {
      this.targetLanguages.add(targetLanguage.getLanguage());
    }
    this.languageDetector = null;
  }

  /**
   * Configures this extractor to extract sentences from paragraphs that are
   * classified as having the specified language.
   * <p>
   * This resets the language detector, so you will have to set it again using
   * {@link #setLanguageDetector(Function)} in case you want to use a different
   * one than the default.
   * </p>
   */
  public final void setExtractLanguage(final Locale targetLanguage) {
    this.setExtractLanguages(Collections.singleton(targetLanguage));
  }

  /**
   * Configures this extractor to extract sentences from all paragraphs and see
   * each paragraph as being written in the given language.  Languages should be
   * ISO codes like "en", "de", and so on.
   * <p>
   * Basically, this does {@link #setExtractLanguage(Locale)} with given
   * language and {@link #setLanguageDetector(Function)} with a function that
   * always returns given language.
   * </p><p>
   * In the case of the basic {@link JerichoHtmlSentenceExtractor}, this has the
   * same effect as calling {@link #setExtractAllLanguages()}. However, if the
   * extractor is extended, it may use the detected language in its decision on
   * what paragraphs to keep. 
   * </p>
   */
  public final void setUseLanguage(final String language) {
    this.setUseLanguage(Locale.forLanguageTag(language));
  }

  /**
   * Configures this extractor to extract sentences from all paragraphs and see
   * each paragraph as being written in the given language.
   * <p>
   * Basically, this does {@link #setExtractLanguage(Locale)} with given
   * language and {@link #setLanguageDetector(Function)} with a function that
   * always returns given language.
   * </p><p>
   * In the case of the basic {@link JerichoHtmlSentenceExtractor}, this has the
   * same effect as calling {@link #setExtractAllLanguages()}. However, if the
   * extractor is extended, it may use the detected language in its decision on
   * what paragraphs or sentences to keep.
   * </p>
   */
  public void setUseLanguage(final Locale language) {
    if (language == null) { throw new NullPointerException(); }
    this.setExtractLanguage(language);
    this.setLanguageDetector(text -> language);
  }
  
  /**
   * Sets the language detector to use on each paragraph. You can use
   * {@link #setUseLanguage(String)} to basically disable language detection.
   * @throws NullPointerException If given language detector is <tt>null</tt>
   */
  public void setLanguageDetector(
      final Function<String, Locale> languageDetector)
  throws NullPointerException {
    if (languageDetector == null) { throw new NullPointerException(); }
    this.languageDetector = languageDetector;
  }
  
  /**
   * Configures this extractor to not add a separate element to the output list
   * (of {@link #extractSentences(String)}) before sentences from a new
   * paragraph.
   */
  public final void setDoNotSeparateParagraphs() {
    this.setParagraphSeparator(null);
    this.separateParagraphs = false;
  }

  /**
   * Configures this extractor to add a separate element to the output list
   * (of {@link #extractSentences(String)}) before sentences from a new
   * paragraph, namely the given one.
   */
  public void setParagraphSeparator(final String paragraphSeparator) {
    this.paragraphSeparator = paragraphSeparator;
    this.separateParagraphs = true;
  }
  
  @Override
  public void configure(final CommandLine config) {
    super.configure(config);
    final String[] targetLanguages =
        config.getOptionValues(FLAG_EXTRACT_LANGUAGES);
    final boolean detectAll =
        config.hasOption(FLAG_EXTRACT_ALL_LANGUAGES);
    final String useLanguage =
        config.getOptionValue(FLAG_USE_LANGUAGE);
    final String paragraphSeparator =
        config.getOptionValue(FLAG_PARAGRAPH_SEPARATOR);
    final boolean doNotSeparateParagraphs =
        config.hasOption(FLAG_DO_NOT_SEPARATE_PARAGRAPHS);
    
    if (detectAll) {
      this.setExtractAllLanguages();
    } else if (targetLanguages != null) {
      this.setExtractLanguages(targetLanguages);
    } else if (useLanguage != null) {
      this.setUseLanguage(useLanguage);
    }
    
    if (doNotSeparateParagraphs) {
      this.setDoNotSeparateParagraphs();
    } else if (paragraphSeparator != null) {
      this.setParagraphSeparator(paragraphSeparator);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  //                               FUNCTIONALITY                              //
  //////////////////////////////////////////////////////////////////////////////

  @Override
  protected List<String> extract(final String htmlInput)
  throws NullPointerException, IllegalArgumentException {
    if (htmlInput == null) {
      throw new NullPointerException();
    }

    final List<String> paragraphs = this.extractParagraphs(htmlInput);
    if (paragraphs == null) {
      throw new IllegalArgumentException("Could not parse: " + htmlInput);
    }
    final List<String> sentences = new ArrayList<>();
    boolean firstParagraph = true;
    for (final String paragraph : paragraphs) {
      final List<String> paragraphSentences =
          this.extractSentencesFromParagraph(paragraph);
      if (!paragraphSentences.isEmpty()) {
        if (firstParagraph) {
          firstParagraph = false;
        } else if (this.separateParagraphs) {
          sentences.add(this.paragraphSeparator);
        }
        sentences.addAll(paragraphSentences);
      }
    }
    return sentences;
  }
  
  /**
   * Renders the HTML page with Jericho {@link Renderer}, normalizes sequences
   * of whitespace characters to a single whitespace, and returns the list of
   * paragraphs. Return <tt>null</tt> on a fatal rendering error.
   */
  protected List<String> extractParagraphs(final String htmlInput) {
    try {
      final Source source = new Source(htmlInput);
      final Segment segment = new Segment(source, 0, htmlInput.length());
      final Renderer renderer = new Renderer(segment);
      renderer.setMaxLineLength(0);
      renderer.setIncludeHyperlinkURLs(false);
      final String[] paragraphsArray = renderer.toString().split("\n");
      final List<String> paragraphs = new ArrayList<>(paragraphsArray.length);
      for (final String paragraph : paragraphsArray) {
        paragraphs.add(this.normalizeWhitespace(paragraph));
      }
      return paragraphs;
    } catch (final Error error) {
      return null;
    }
  }

  /**
   * Detects the language of the paragraph, checks whether it is a target
   * language and it {@link #isValidParagraph(String, Locale)}, and returns the
   * sentences from it. Returns an empty list when the paragraph is empty, from
   * a non-target language, or not valid.
   */
  protected List<String> extractSentencesFromParagraph(final String paragraph) {
    final Locale paragraphLanguage = this.detectLanguage(paragraph);
    if (paragraphLanguage == null
        || !this.isValidParagraph(paragraph, paragraphLanguage)) {
      return Collections.emptyList();
    }
    return this.extractSentencesFromParagraph(paragraph, paragraphLanguage);
  }

  /**
   * Extract sentence from the given paragraph of given language. This is
   * called after it was checked that the paragraph is in a target language and
   * valid.
   */
  protected List<String> extractSentencesFromParagraph(
      final String paragraph, final Locale paragraphLanguage) {
    // they are not thread-safe, so we create a new one each time
    final BreakIterator segmenter =
        BreakIterator.getSentenceInstance(paragraphLanguage);

    final List<String> sentences = new ArrayList<String>();
    for (final String sentence : this.getSegments(paragraph, segmenter)) {
      if (!sentence.isEmpty()) {
        if (this.isValidSentence(sentence, paragraphLanguage)) {
          sentences.add(sentence);
        }
      }
    }
    return sentences;
  }
  
  /**
   * Checks whether given paragraph of given language should be extracted.
   * <p>
   * The default implementation of this method always return true.
   * </p>
   */
  protected boolean isValidParagraph(
      final String paragraph, final Locale paragraphLanguage) {
    return true;
  }
  
  /**
   * Checks whether given sentence of given language should be extracted.
   * <p>
   * The sentence is from a paragraph of given language that is valid according
   * to {@link #isValidParagraph(String, Locale)}.
   * </p><p>
   * The default implementation of this method always return true.
   * </p>
   */
  protected boolean isValidSentence(
      final String sentence, final Locale paragraphLanguage) {
    return true;
  }
  
  /**
   * Detects the language of the text using the language detector (see
   * {@link #setLanguageDetector(Function)}) and return the corresponding locale
   * if it is a target language (see {@link #setExtractLanguages(Collection)}).
   * Returns <tt>null</tt> if the language can not be detected or it is not a
   * target language.
   */
  protected Locale detectLanguage(final String text) {
    final Locale detectedLanguage = this.getLanguageDetector().apply(text);
    if (!this.isTargetLanguage(detectedLanguage)) { return null; }
    return detectedLanguage;
  }

  /**
   * Checks whether the given language is a target language.
   * @see #setExtractLanguages(Locale...)
   */
  protected boolean isTargetLanguage(final Locale language) {
    if (language == null) { return false; }

    return this.targetLanguages == null
        || this.targetLanguages.contains(language.getLanguage());
  }

  /**
   * Normalizes sequences of whitespace characters and trims the text.
   */
  protected String normalizeWhitespace(final String text) {
    return text.replaceAll("\\s+", " ").trim();
  }
  
  /**
   * Uses given break iterator to segment the text.
   */
  protected List<String> getSegments(
      final String text, final BreakIterator segmenter) {
    segmenter.setText(text);

    final List<String> segments = new ArrayList<>();
    int begin = segmenter.first();
    int end = segmenter.next();
    while (end != BreakIterator.DONE) {
      segments.add(text.substring(begin, end).trim());
      begin = end;
      end = segmenter.next();
    }

    return segments;
  }

  //////////////////////////////////////////////////////////////////////////////
  //                                   PROGRAM                                //
  //////////////////////////////////////////////////////////////////////////////
  
  @Override
  public Options addOptions(final Options options) {
    super.addOptions(options);
    final OptionGroup languages = new OptionGroup();
    final Option targetLanguagesOption =
        new Option(SHORT_FLAG_EXTRACT_LANGUAGES, true,
            "Configures this extractor to extract sentences from paragraphs "
            + "that are classified as having the specified languages. "
            + "Languages should be ISO codes like 'en', 'de', and so on and "
            + "should be provided as comma-separated list (e.g., 'en,de')");
    targetLanguagesOption.setLongOpt(FLAG_EXTRACT_LANGUAGES);
    targetLanguagesOption.setArgs(Option.UNLIMITED_VALUES);
    targetLanguagesOption.setValueSeparator(',');
    targetLanguagesOption.setArgName("lang,lang,...");
    languages.addOption(targetLanguagesOption);
    
    final Option allLanguagesOption =
        new Option(SHORT_FLAG_EXTRACT_ALL_LANGUAGES, false,
            "Configures this extractor to extract sentences from paragraphs of "
            + "all languages");
    allLanguagesOption.setLongOpt(FLAG_EXTRACT_ALL_LANGUAGES);
    languages.addOption(allLanguagesOption);
    
    final Option useLanguageOption = new Option(SHORT_FLAG_USE_LANGUAGE, true,
        "Configures this extractor to extract sentences from all paragraphs "
        + "and see each paragraph as being written in the given language "
        + "without doing a language detection. The language should be an ISO "
        + "code like 'en', 'de', and so on");
    useLanguageOption.setLongOpt(FLAG_USE_LANGUAGE);
    useLanguageOption.setArgName("lang");
    languages.addOption(useLanguageOption);
    options.addOptionGroup(languages);
    
    
    
    final OptionGroup paragraphs = new OptionGroup();
    final Option paragraphSeparatorOption = new Option(
        SHORT_FLAG_PARAGRAPH_SEPARATOR, true,
            "Configures this extractor to add a separate line between "
            + "sentences from a new paragraph that contains <sep>");
    paragraphSeparatorOption.setLongOpt(FLAG_PARAGRAPH_SEPARATOR);
    paragraphSeparatorOption.setArgName("sep");
    paragraphs.addOption(paragraphSeparatorOption);
    
    final Option paragraphNotSeparateOption = new Option(
        SHORT_FLAG_DO_NOT_SEPARATE_PARAGRAPHS, false,
        "Configures this extractor to not add a separate line between "
        + "sentences from a new paragraph");
    paragraphNotSeparateOption.setLongOpt(FLAG_DO_NOT_SEPARATE_PARAGRAPHS);
    paragraphs.addOption(paragraphNotSeparateOption);
    options.addOptionGroup(paragraphs);

    return options;
  }
  
  public static void main(final String[] args) throws Exception {
    HtmlSentenceExtractor.main(args, JerichoHtmlSentenceExtractor.class);
  }

}
