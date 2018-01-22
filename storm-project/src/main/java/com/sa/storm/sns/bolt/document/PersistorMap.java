/**
 * 
 */
package com.sa.storm.sns.bolt.document;

import java.util.HashMap;

import social.hunt.crawler.persist.ArticlePersistor;
import social.hunt.crawler.persist.DiscussionPersistor;
import social.hunt.crawler.persist.FacebookPersistor;
import social.hunt.crawler.persist.InstagramPersistor;
import social.hunt.crawler.persist.Persistor;
import social.hunt.crawler.persist.PlurkPersistor;
import social.hunt.crawler.persist.PublicSearchPersistor;
import social.hunt.crawler.persist.TelegramPersistor;
import social.hunt.crawler.persist.TwitterPersistor;
import social.hunt.crawler.persist.WeiboPersistor;
import social.hunt.crawler.persist.WeixinPersistor;
import social.hunt.crawler.persist.YoutubePersistor;

import com.sa.common.definition.SourceType;

/**
 * @author lewis
 *
 */
@SuppressWarnings("deprecation")
public class PersistorMap extends HashMap<SourceType, Persistor> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.HashMap#get(java.lang.Object)
	 */
	@Override
	public Persistor get(Object key) {
		Persistor obj = super.get(key);
		if (obj == null) {
			SourceType sourceType = (SourceType) key;

			switch (sourceType) {
			case FACEBOOK:
				obj = new FacebookPersistor();
				break;
			case FORUM:
				obj = new DiscussionPersistor();
				break;
			case INSTAGRAM:
				obj = new InstagramPersistor();
				break;
			case NEWS:
				obj = new ArticlePersistor();
				break;
			case SINA:
				obj = new WeiboPersistor();
				break;
			case SOUGO_WEIXIN:
				obj = new WeixinPersistor();
				break;
			case TWITTER:
				obj = new TwitterPersistor();
				break;
			case YOUTUBE:
				obj = new YoutubePersistor();
				break;
			case OTHERS:
				obj = new PublicSearchPersistor();
				break;
			case TELEGRAM:
				obj = new TelegramPersistor();
				break;
			case PLURK:
				obj = new PlurkPersistor();
				break;
			default:
				return null;
			}
			this.put(sourceType, obj);
		}
		return obj;
	}

}
