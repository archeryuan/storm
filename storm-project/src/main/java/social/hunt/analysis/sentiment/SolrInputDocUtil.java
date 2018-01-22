/**
 * 
 */
package social.hunt.analysis.sentiment;

import java.util.Collection;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.definition.SolrFieldDefinition;

/**
 * @author Jason
 *
 */
public class SolrInputDocUtil {

	private static final Logger log = LoggerFactory.getLogger(SolrInputDocUtil.class);

	/**
	 * Extract and concat. title & content from SolrInputDocument
	 * 
	 * @param doc
	 * @return
	 */
	public String extractTitleContent(SolrInputDocument doc) {
		StringBuilder sb = new StringBuilder();

		if (doc.containsKey(SolrFieldDefinition.TITLE.getName())) {
			sb.append(StringUtils.trimToEmpty((String) doc.getFieldValue(SolrFieldDefinition.TITLE.getName())));
			if (sb.length() > 0)
				sb.append(System.lineSeparator()).append(System.lineSeparator());
		}

		if (doc.containsKey(SolrFieldDefinition.CONTENT.getName())) {
			Object obj = doc.getFieldValue(SolrFieldDefinition.CONTENT.getName());
			if (obj != null) {
				if (obj instanceof Collection) {
					@SuppressWarnings("unchecked")
					Collection<String> coll = (Collection<String>) obj;
					for (String content : coll) {
						sb.append(StringUtils.trimToEmpty(content)).append(System.lineSeparator());
						break;
					}
				} else if (obj instanceof String) {
					if (StringUtils.isBlank((String) obj)) {
						log.info("Title:{}", sb.toString());
						// Feed do not contain content field
//						return null;
					} else {
						sb.append(StringUtils.trimToEmpty((String) obj));
					}
				}
			} else {
				// Feed do not contain content field
				log.info("Title:{}", sb.toString());
//				return null;
			}
		} else {
			// Feed do not contain content field
			log.info("Title:{}", sb.toString());
//			return null;
		}
		String res = sb.toString();
		if(res.length() > 800) {
			res = res.substring(0, 800);
		}

		return StringUtils.trimToEmpty(res);
	}

	/**
	 * Generate unique ID for Semantria call with document URL
	 * 
	 * @param sDoc
	 * @return
	 */
	public String generateDocId(SolrInputDocument sDoc) {
		String url = (String) sDoc.getFieldValue(SolrFieldDefinition.URL.getName());

		// The docId can only contains 32 chars
		return DigestUtils.md5Hex(url);
	}

}
