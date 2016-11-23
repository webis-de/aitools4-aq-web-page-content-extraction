package de.aitools.aq.web.extractor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.entity.DecompressingEntity;
import org.apache.http.client.entity.DeflateInputStream;
import org.apache.http.client.entity.InputStreamFactory;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentLengthStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.entity.LaxContentLengthStrategy;
import org.apache.http.impl.io.ChunkedInputStream;
import org.apache.http.impl.io.ContentLengthInputStream;
import org.apache.http.impl.io.DefaultHttpResponseParser;
import org.apache.http.impl.io.EmptyInputStream;
import org.apache.http.impl.io.HttpTransportMetricsImpl;
import org.apache.http.impl.io.IdentityInputStream;
import org.apache.http.impl.io.SessionInputBufferImpl;
import org.apache.http.io.SessionInputBuffer;
// Current hadoop uses version of httpcomponents where they were the way to go
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import edu.cmu.lemurproject.WarcRecord;

/**
 * Utility class for handling WARC files.
 *
 * @author johannes.kiesel@uni-weimar.de
 * @version $Date: 2016/11/23 10:47:26 $
 *
 */
@SuppressWarnings("deprecation")
public class Warcs {
  
  private static final Logger LOGGER =
      Logger.getLogger(Warcs.class.getName());
  
  private static final Pattern HTML_CONTENT_TYPE_PATTERN = Pattern.compile(
      "text/html.*");

  private static final String HEADER_CONTENT_TYPE = "Content-Type";

  private final static InputStreamFactory GZIP = new InputStreamFactory() {
    @Override
    public InputStream create(final InputStream instream) throws IOException {
      return new GZIPInputStream(instream);
    }
  };

  private final static InputStreamFactory DEFLATE = new InputStreamFactory() {
    @Override
    public InputStream create(final InputStream instream) throws IOException {
      return new DeflateInputStream(instream);
    }
  };
  
  private Warcs() { }

  /**
   * Reads the WARC records from given input. If the file name ends in .gz, the
   * WARC will be decompressed. 
   */  
  public static Stream<WarcRecord> getRecords(final File input)
  throws IOException {
    final InputStream inputStream = new FileInputStream(input);
    if (input.getName().endsWith(".gz")) {
      return Warcs.getRecords(new GZIPInputStream(inputStream));
    } else {
      return Warcs.getRecords(inputStream);
    }
  }

  /**
   * Reads the WARC records from given input.
   */  
  public static Stream<WarcRecord> getRecords(final InputStream input)
  throws IOException {
    return Warcs.getRecords(new DataInputStream(input));
  }

  /**
   * Reads the WARC records from given input.
   */
  public static Stream<WarcRecord> getRecords(final DataInputStream input)
  throws IOException {
    final List<WarcRecord> records = new ArrayList<>();
    WarcRecord record = WarcRecord.readNextWarcRecord(input);
    while (record != null) {
      records.add(record);
      record = WarcRecord.readNextWarcRecord(input);
    }
    input.close();
    return records.stream();
  }

  /**
   * Reads the WARC records from given input and extracts them using
   * {@link #getHtml(WarcRecord)}. If the file name ends in .gz, the WARC will
   * be decompressed. 
   */
  public static Stream<String> getHtmlFromRecords(final File input)
  throws IOException {
    final InputStream inputStream = new FileInputStream(input);
    if (input.getName().endsWith(".gz")) {
      return Warcs.getHtmlFromRecords(new GZIPInputStream(inputStream));
    } else {
      return Warcs.getHtmlFromRecords(inputStream);
    }
  }

  /**
   * Reads the WARC records from given input and extracts them using
   * {@link #getHtml(WarcRecord)}.
   */
  public static Stream<String> getHtmlFromRecords(final InputStream input)
  throws IOException {
    return Warcs.getHtmlFromRecords(new DataInputStream(input));
  }

  /**
   * Reads the WARC records from given input and extracts them using
   * {@link #getHtml(WarcRecord)}.
   */
  public static Stream<String> getHtmlFromRecords(final DataInputStream input)
  throws IOException {
    return Warcs.getRecords(input)
        .map(record -> {
            try {
              return Warcs.getHtml(record);
            } catch (final IOException e) {
              LOGGER.warning(e.getMessage());
              throw new UncheckedIOException(e);
            } catch (final Exception e) {
              LOGGER.warning(e.getMessage());
              throw new RuntimeException(e);
            }
          })
        .filter(record -> record != null);
  }
  
  /**
   * Gets the HTML part of a record or <tt>null</tt> if there is none or an
   * invalid one.
   */
  public static String getHtml(final WarcRecord record)
  throws ParseException, IOException, HttpException {
    final HttpResponse response = Warcs.toResponse(record);
    if (response == null) { return null; }
    final String contentType =
        response.getLastHeader(HEADER_CONTENT_TYPE).getValue();
    if (contentType == null) { return null; }
    if (!HTML_CONTENT_TYPE_PATTERN.matcher(contentType).matches()) {
      return null;
    }
    final HttpEntity entity = response.getEntity();
    final String defaultCharset = null;
    return EntityUtils.toString(entity, defaultCharset);
  }

  /**
   * Gets an {@link HttpResponse} object from a WARC record of such a response.
   * @return The response or <tt>null</tt> when the record is not a response
   * record
   */
  public static HttpResponse toResponse(final WarcRecord record)
  throws IOException, HttpException {
    // based on http://stackoverflow.com/a/26586178
    if (!record.getHeaderRecordType().equals("response")) { return null; }
    
    final SessionInputBufferImpl sessionInputBuffer =
        new SessionInputBufferImpl(new HttpTransportMetricsImpl(), 2048);
    final InputStream inputStream =
        new ByteArrayInputStream(record.getByteContent());
    sessionInputBuffer.bind(inputStream);
    final HttpParams params = new BasicHttpParams();
    final DefaultHttpResponseParser parser =
        new DefaultHttpResponseParser(
            sessionInputBuffer, null, new DefaultHttpResponseFactory(),
            params);
    final HttpResponse response = parser.parse();
    final HttpEntity entity = Warcs.getEntity(response, sessionInputBuffer);
    response.setEntity(entity);
    Warcs.encodeEntity(response);
    return response;
  }
  
  private static void encodeEntity(final HttpResponse response)
  throws HttpException, IOException {
    // Adapted from org.apache.http.client.protocol.ResponseContentEncoding
    final HttpEntity entity = response.getEntity();
  
    // entity can be null in case of 304 Not Modified, 204 No Content or similar
    // check for zero length entity.
    if (entity != null && entity.getContentLength() != 0) {
      final Header ceheader = entity.getContentEncoding();
      if (ceheader != null) {
        final HeaderElement[] codecs = ceheader.getElements();
        final Lookup<InputStreamFactory> decoderRegistry =
            RegistryBuilder.<InputStreamFactory>create()
              .register("gzip", GZIP)
              .register("x-gzip", GZIP)
              .register("deflate", DEFLATE)
              .build();
        for (final HeaderElement codec : codecs) {
          final String codecname = codec.getName().toLowerCase(Locale.ROOT);
          final InputStreamFactory decoderFactory =
              decoderRegistry.lookup(codecname);
          if (decoderFactory != null) {
            response.setEntity(new DecompressingEntity(
                response.getEntity(), decoderFactory));
            response.removeHeaders("Content-Length");
            response.removeHeaders("Content-Encoding");
            response.removeHeaders("Content-MD5");
          } else {
            if (!"identity".equals(codecname)) {
                throw new HttpException(
                    "Unsupported Content-Encoding: " + codec.getName());
            }
          }
        }
      }
    }
}
  
  private static InputStream createInputStream(
      final long len, final SessionInputBuffer input) {
    // Adapted from the org.apache.http.impl.BHttpConnectionBase
    if (len == ContentLengthStrategy.CHUNKED) {
      return new ChunkedInputStream(input);
    } else if (len == ContentLengthStrategy.IDENTITY) {
      return new IdentityInputStream(input);
    } else if (len == 0L) {
      return EmptyInputStream.INSTANCE;
    } else {
      return new ContentLengthInputStream(input, len);
    }
  }
  
  private static HttpEntity getEntity(
      final HttpResponse response, final SessionInputBuffer input)
  throws HttpException {
    // Adapted from the org.apache.http.impl.BHttpConnectionBase
    final BasicHttpEntity entity = new BasicHttpEntity();

    final long len =
        new LaxContentLengthStrategy().determineLength(response);
    final InputStream instream = Warcs.createInputStream(len, input);
    if (len == ContentLengthStrategy.CHUNKED) {
      entity.setChunked(true);
      entity.setContentLength(-1);
      entity.setContent(instream);
    } else if (len == ContentLengthStrategy.IDENTITY) {
      entity.setChunked(false);
      entity.setContentLength(-1);
      entity.setContent(instream);
    } else {
      entity.setChunked(false);
      entity.setContentLength(len);
      entity.setContent(instream);
    }

    final Header contentTypeHeader = 
        response.getFirstHeader(HTTP.CONTENT_TYPE);
    if (contentTypeHeader != null) {
      entity.setContentType(contentTypeHeader);
    }
    final Header contentEncodingHeader =
        response.getFirstHeader(HTTP.CONTENT_ENCODING);
    if (contentEncodingHeader != null) {
      entity.setContentEncoding(contentEncodingHeader);
    }
    return entity;
  }

}
