AItools 4 - Acquisition - Web Page Content Extraction
=====================================================

Library and command line program to extract main content sentences from web pages. The program can run both locally and with hadoop:

    java -cp aitools4-aq-web-page-content-extraction-bin.jar de.aitools.aq.web.extractor.PotthastJerichoExtractor local --help

    java -cp aitools4-aq-web-page-content-extraction-bin.jar de.aitools.aq.web.extractor.PotthastJerichoExtractor local --input foo.html,bar.warc.gz --output out

    hadoop jar aitools4-aq-web-page-content-extraction-bin.jar de.aitools.aq.web.extractor.PotthastJerichoExtractor hadoop --input foo.html,bar.warc.gz --output out

See the documentation of de.aitools.aq.web.extractor.PotthastJerichoExtractor for more information. To extract all text from a web page, use de.aitools.aq.web.extractor.JerichoHtmlSentenceExtractor.

The default settings are the ones used in
<pre>
Johannes Kiesel, Benno Stein, and Stefan Lucks.
A Large-scale Analysis of the Mnemonic Password Advice.
In Proceedings of the 24th Annual Network and Distributed System Security Symposium (NDSS 17),
February 2017. 
</pre>

Dependencies (packed into the aitools4-aq-web-page-content-extraction-bin.jar)
------------------------------------------------------------------------------
  - aitools3-ie-languagedetection (available on request)
  - aitools3-ie-stopwords (available on request)
  - apache-commons-cli-1.2
  - apache-hadoop-2.5.2
  - apache-httpcomponents-client-4.5.2
  - icu4j-4.8.1.1
  - jericho-html-3.3
  - [Lemur project WARC classes](http://www.lemurproject.org/clueweb09/workingWithWARCFiles.php) (for WARC files and Hadoop)

