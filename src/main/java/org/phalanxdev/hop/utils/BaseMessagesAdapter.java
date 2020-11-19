package org.phalanxdev.hop.utils;

import org.apache.hop.i18n.BaseMessages;
import org.phalanxdev.mi.utils.IMIMessages;

/**
 * Delegate for hop i18 BaseMessages
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version 1: $
 */
public class BaseMessagesAdapter implements IMIMessages {

  protected Class<?> m_baseClazz;

  public BaseMessagesAdapter(Class<?> sourceClazz) {
    m_baseClazz = sourceClazz;
  }

  @Override
  public String getString(String s) {
    return BaseMessages.getString(m_baseClazz, s);
  }

  @Override
  public String getString(String s, String... strings) {
    return BaseMessages.getString(m_baseClazz, s, strings);
  }

  @Override
  public String getString(String s, Object... objects) {
    return BaseMessages.getString(m_baseClazz, s, objects);
  }
}
