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

import java.util.ArrayList;
import java.util.List;

import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

/**
 * Contains the meta data for one field being converted to ARFF
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}org)
 * @version $Revision: 1.0 $
 */
public class ArffMeta implements Cloneable {

  // some constants for ARFF data types
  public static final int NUMERIC = 0;
  public static final int NOMINAL = 1;
  public static final int DATE = 2;
  public static final int STRING = 3;

  // the name of this field
  private final String m_fieldName;

  // the Apache Hop data type (as defined in ValueMetaInterface)
  private int m_hopType;

  // the ARFF data type (as defined by the constants above)
  private int m_arffType;

  // the format for date fields
  private String m_dateFormat;

  private String m_nominalVals;

  public static final String XML_TAG = "arff_field";

  /**
   * Constructor
   * 
   * @param fieldName the name of this field
   * @param hopType the Apache Hop data type
   * @param arffType the ARFF data type
   */
  public ArffMeta(String fieldName, int hopType, int arffType) {

    m_fieldName = fieldName;
    m_hopType = hopType;
    m_arffType = arffType;
    // m_precision = precision;
  }
  
  /**
   * Construct from an XML node
   * 
   * @param arffNode a XML node
   */
  public ArffMeta(Node arffNode) {
    String temp;
    m_fieldName = XmlHandler.getTagValue(arffNode, "field_name");
    temp = XmlHandler.getTagValue(arffNode, "kettle_type");
    try {
      m_hopType = Integer.parseInt(temp);
    } catch (Exception ex) {
      // ignore - shouldn't actually get here
    }
    temp = XmlHandler.getTagValue(arffNode, "arff_type");
    try {
      m_arffType = Integer.parseInt(temp);
    } catch (Exception ex) {
      // ignore
    }

    m_dateFormat = XmlHandler.getTagValue(arffNode, "date_format");
    m_nominalVals = XmlHandler.getTagValue(arffNode, "nominal_vals");
  }

  /**
   * Get the name of this field
   * 
   * @return the name of the field
   */
  public String getFieldName() {
    return m_fieldName;
  }

  /**
   * Get the Apache Hop data type (as defined in ValueMetaInterface)
   * 
   * @return the Apache Hop data type of this field
   */
  public int getHopType() {
    return m_hopType;
  }

  /**
   * Get the ARFF data type
   * 
   * @return the ARFF data type of this field
   */
  public int getArffType() {
    return m_arffType;
  }

  public void setDateFormat(String dateFormat) {
    m_dateFormat = dateFormat;
  }

  public String getDateFormat() {
    return m_dateFormat;
  }

  /**
   * Set a comma-separated list of legal values for a nominal attribute
   * 
   * @param vals the legal values
   */
  public void setNominalVals(String vals) {
    m_nominalVals = vals;
  }

  /**
   * Get a comma-separated list of legal values for a nominal attribute
   * 
   * @return the legal values
   */
  public String getNominalVals() {
    return m_nominalVals;
  }

  /**
   * Make a copy
   * 
   * @return a copy of this UnivariateStatsMetaFunction.
   */
  @Override
  public Object clone() {
    try {
      ArffMeta retval = (ArffMeta) super.clone();

      return retval;
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  /**
   * Get the XML representation of this field
   * 
   * @return a <code>String</code> value
   */
  public String getXML() {
    String xml = ("<" + XML_TAG + ">");

    xml += XmlHandler.addTagValue("field_name", m_fieldName);
    xml += XmlHandler.addTagValue("kettle_type", m_hopType);
    xml += XmlHandler.addTagValue("arff_type", m_arffType);
    xml += XmlHandler.addTagValue("date_format", m_dateFormat);
    xml += XmlHandler.addTagValue("nominal_vals", m_nominalVals);

    xml += ("</" + XML_TAG + ">");

    return xml;
  }

  public static List<String> stringToVals(String vals) {
    List<String> nomVals = new ArrayList<String>();

    if (!org.apache.hop.core.util.Utils.isEmpty(vals)) {
      String[] parts = vals.split(",");
      for (String p : parts) {
        nomVals.add(p.trim());
      }
    }

    return nomVals;
  }
}
