/*
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.phalanxdev.hop.utils;

import java.io.PrintStream;
import java.io.Serializable;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.IMetrics;
import org.apache.hop.core.logging.LogLevel;

import org.phalanxdev.mi.utils.IMILogAdapter;
import weka.gui.Logger;

/**
 * Adapts Hop logging to Weka's Logger interface and PrintStream. Allows Weka's logging
 * to get passed through to Hop
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: 1.0 $
 */
public class LogAdapter extends PrintStream 
  implements Serializable, Logger, ILogChannel, IMILogAdapter {

  
  /**
   * For serialization
   */
  private static final long serialVersionUID = 4861213857483800216L;
  
  private transient ILogChannel m_log;

  public LogAdapter(ILogChannel log) {
    super(System.out);
    m_log = log;
  }

  /**
   * Weka Logger method
   * 
   * @param message log message for the status area
   */
  public void statusMessage(String message) {
    m_log.logDetailed(message);
  }

  /**
   * Weka Logger method
   * 
   * @param message log message for the log area
   */
  public void logMessage(String message) {
    m_log.logBasic(message);
  }
  
  /**
   * PrintStream method
   * 
   * @param string the log message
   */
  public void println(String string) {
    // make sure that the global weka log picks it up
    System.out.println(string);    
    statusMessage(string);
  }
  
  /**
   * PrintStream method
   * 
   * @param obj the log message
   */
  public void println(Object obj) {
    println(obj.toString());
  }
  
  /**
   * PrintStream method
   * 
   * @param string the log message
   */
  public void print(String string){
    // make sure that the global weka log picks it up
    System.out.print(string);
    statusMessage(string);      
  }
  
  /**
   * PrintStream method
   * 
   * @param obj the log message
   */
  public void print(Object obj) {
    print(obj.toString());
  }

  @Override
  public String getContainerObjectId() {
    return m_log.getContainerObjectId();
  }

  @Override
  public String getLogChannelId() {
    return m_log.getLogChannelId();
  }

  @Override
  public LogLevel getLogLevel() {
    return m_log.getLogLevel();
  }

  @Override
  public boolean isBasic() {
    return m_log.isBasic();
  }

  @Override
  public boolean isDebug() {
    return m_log.isDebug();
  }

  @Override
  public boolean isDetailed() {
    return m_log.isDetailed();
  }

  @Override
  public boolean isError() {
    return m_log.isError();
  }

  @Override
  public boolean isRowLevel() {
    return m_log.isRowLevel();
  }

  @Override
  public void logBasic(String arg0) {
    m_log.logBasic(arg0);
  }

  @Override
  public void logBasic(String arg0, Object... arg1) {
    m_log.logBasic(arg0, arg1);    
  }

  @Override
  public void logDebug(String arg0) {
    m_log.logDebug(arg0);
  }

  @Override
  public void logDebug(String arg0, Object... arg1) {
    m_log.logDebug(arg0, arg1);
  }

  @Override
  public void logDetailed(String arg0) {
    m_log.logDetailed(arg0);
  }

  @Override
  public void logDetailed(String arg0, Object... arg1) {
    m_log.logDetailed(arg0, arg1);
  }

  @Override
  public void logError(String arg0) {
    m_log.logError(arg0);
  }

  @Override
  public void logError(String arg0, Throwable arg1) {
    m_log.logError(arg0, arg1);    
  }

  @Override
  public void logError(String arg0, Object... arg1) {
    m_log.logError(arg0, arg1);
  }

  @Override
  public void logMinimal(String arg0) {
    m_log.logMinimal(arg0);
  }

  @Override
  public void logMinimal(String arg0, Object... arg1) {
    m_log.logMinimal(arg0, arg1);
  }

  @Override
  public void logRowlevel(String arg0) {
    m_log.logRowlevel(arg0);
  }

  @Override
  public void logRowlevel(String arg0, Object... arg1) {
    m_log.logRowlevel(arg0, arg1);
  }

  @Override
  public void setContainerObjectId(String arg0) {
    m_log.setContainerObjectId(arg0);    
  }

  @Override
  public void setLogLevel(LogLevel arg0) {
    m_log.setLogLevel(arg0);    
  }

  @Override
  public void snap(IMetrics metric, long... value) {
    m_log.snap(metric, value);
  }

  @Override
  public void snap(IMetrics metric, String subject, long... value) {
    m_log.snap(metric, subject, value);
  }

  @Override
  public boolean isForcingSeparateLogging() {
    return m_log.isForcingSeparateLogging();
  }

  @Override
  public void setForcingSeparateLogging(boolean forcingSeparateLogging) {
    m_log.setForcingSeparateLogging(forcingSeparateLogging);
  }

  @Override
  public boolean isGatheringMetrics() {
    return m_log.isGatheringMetrics();
  }

  @Override
  public void setGatheringMetrics(boolean gatheringMetrics) {
    m_log.setGatheringMetrics(gatheringMetrics);
  }

  @Override
  public String getFilter() {
    return m_log.getFilter();
  }

  @Override
  public void setFilter(String filter) {
    m_log.setFilter(filter);
  }
}
