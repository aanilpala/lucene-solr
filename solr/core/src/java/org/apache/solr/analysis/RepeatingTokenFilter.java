package org.apache.solr.analysis;

import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.core.SolrCore;

//analyser filter for considering the last payload value as the term frequency
public class RepeatingTokenFilter extends TokenFilter {
    private final char              delimiter;
    private final CharTermAttribute termAtt           = addAttribute(CharTermAttribute.class);

    private int                     repeatCount       = -1;

    public RepeatingTokenFilter(TokenStream input, char delimiter) {
        super(input);
        this.delimiter = delimiter;
    }

    // optimised to get the last token
    public int GetRepeatCount(char[] buffer, int offset, int length) {
        int iFirstPos = offset - 1;// One less than the previous delimiter index
        int iStartingLastPos = length - 2;// second last index; to avoid checking
                                          // delimiter in the last index.
        for (int i = iStartingLastPos; i > iFirstPos; i--) {
            if (buffer[i] == delimiter) {
                String sToken = new String(buffer, i + 1, iStartingLastPos + 1
                        - i);
                return Integer.parseInt(sToken);
            }
        }
        return -1;
    }

    @Override
    public final boolean incrementToken() throws IOException {
        if (repeatCount == -1) {
            if (input.incrementToken()) {
                final char[] buffer = termAtt.buffer();
                final int length = termAtt.length();

                for (int i = 0; i < length; i++) {
                    if (buffer[i] == delimiter) {
                        repeatCount = GetRepeatCount(buffer, i, length);
                        if (repeatCount > 0) {
                            repeatCount--;
                            termAtt.setLength(i); // truncate "term" to remove all the relevance values
                            return true;
                        } else if (repeatCount == 0) {
                            SolrCore.log.warn("0 relevance value found in term value: " + termAtt.toString());
                            termAtt.setLength(i); // truncate "term" to remove all the relevance values
                            return false;
                        } else {
                            repeatCount = -1;
                            break;
                        }
                    }
                }
                // if payload is not found
                SolrCore.log.error("failed to get payload value from term: " + termAtt.toString());
                return false;
            } else {
                // increment token chain failed
                SolrCore.log.warn("increment token chain failed");
                return false;
            }
        } else if (repeatCount-- > 0) {
            return true;
        } else {
            // End of repeating the same token
            return false;
        }
    }

    @Override
    public void reset() throws IOException {
      super.reset();
      repeatCount = -1;
    }
}
