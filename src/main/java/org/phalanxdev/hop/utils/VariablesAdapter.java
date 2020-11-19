package org.phalanxdev.hop.utils;

import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.phalanxdev.mi.utils.IMIVariableAdaptor;

/**
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version 1: $
 */
public class VariablesAdapter implements IMIVariableAdaptor {

  protected IVariables m_delegate;

  public VariablesAdapter(IVariables delegate) {
    m_delegate = delegate;
  }

  @Override
  public List<String> listVariables() {
    return Arrays.asList( m_delegate.listVariables() );
  }

  @Override
  public String getVariable(String s) {
    return m_delegate.getVariable(s);
  }
}
