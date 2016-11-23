package de.aitools.aq.text;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import de.aitools.ie.stopwords.StopWordList;

/**
 * Filters words that are not stop words.
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date: 2016/11/22 11:49:40 $
 *
 */
public class StopWordFilter extends WordFilter {
  
  protected static final Set<String> NO_LIST_AVAILABLE = null;

  private final Map<Locale, StopWordPredicate> stopWordLists;
  
  private final boolean ignoreCase;

  /**
   * Create a new filter that discards all words that do not match a stop word
   * in the stop word list (ignoring case) of the respective language.
   */
  public StopWordFilter() {
    this(true);
  }

  /**
   * Create a new filter that discards all words that do not match a stop word
   * in the stop word list of the respective language.
   * @param ignoreCase Whether stop words should be checked ignoring case
   */
  public StopWordFilter(final boolean ignoreCase) {
    this.stopWordLists = new HashMap<>();
    this.ignoreCase = ignoreCase;
  }
  
  /**
   * Checks whether this filter ignores the case of the stop words when
   * matching.
   */
  public boolean getIgnoresCase() {
    return this.ignoreCase;
  }

  /**
   * Adds given stop words to the list for the given language.
   */
  public void addStopWords(
      final Locale language, final Iterable<String> words) {
    if (language == null) { throw new NullPointerException(); }
    if (words == null) { throw new NullPointerException(); }
    StopWordPredicate predicate = this.stopWordLists.get(language);
    if (predicate == null) {
      predicate = new StopWordPredicate(language);
      this.stopWordLists.put(language, predicate);
    }
    predicate.addStopWords(words);
  }

  /**
   * Adds given stop words to the list for the given language.
   */
  public void addStopWords(
      final Locale language, final String[] words) {
    if (language == null) { throw new NullPointerException(); }
    if (words == null) { throw new NullPointerException(); }
    StopWordPredicate predicate = this.stopWordLists.get(language);
    if (predicate == null) {
      predicate = new StopWordPredicate(language);
      this.stopWordLists.put(language, predicate);
    }
    predicate.addStopWords(words);
  }

  /**
   * Clears all stop word lists except those for the given languages.
   */
  public void retainStopWordLists(final Collection<Locale> languages) {
    this.stopWordLists.keySet().removeIf(
        language -> !languages.contains(language));
  }
 
  /**
   * Clears all stop word lists.
   */
  public void clearStopWordLists() {
    this.stopWordLists.clear();
  }

  /**
   * Removes the stop word lists of the given language.
   */
  public void removeStopWords(final Locale language) {
    this.stopWordLists.remove(language);
  }

  @Override
  public Predicate<String> getPredicate(final Locale language) {
    final Predicate<String> predicate = this.stopWordLists.get(language);
    if (predicate != null) {
      return predicate;
    } else if (this.stopWordLists.containsKey(language)) {
      throw new IllegalArgumentException("Language not supported: " + language);
    } else {
      synchronized (this.stopWordLists) {
        if (!this.stopWordLists.containsKey(language)) {
          try {
            final String[] stopWords =
                new StopWordList(language).getStopWordList();
            this.addStopWords(language, stopWords);
          } catch (final Error e) {
            this.stopWordLists.put(language, null);
          }
        }
      }
      return this.getPredicate(language);
    }
  }

  protected class StopWordPredicate implements Predicate<String> {
    
    private final Set<String> stopWords;
    
    private final Locale language;
    
    public StopWordPredicate(final Locale language) {
      if (language == null) { throw new NullPointerException(); }
      this.stopWords = new HashSet<>();
      this.language = language;
    }

    @Override
    public boolean test(final String word) {
      return this.stopWords.contains(this.normalize(word));
    }
    
    protected void addStopWords(final String[] words) {
      for (final String word : words) {
        this.stopWords.add(this.normalize(word));
      }
    }
    
    protected void addStopWords(final Iterable<String> words) {
      for (final String word : words) {
        this.stopWords.add(this.normalize(word));
      }
    }
    
    protected String normalize(final String word) {
      if (StopWordFilter.this.ignoreCase) {
        return word.toLowerCase(this.language);
      } else {
        return word;
      }
    }
    
  }

}
