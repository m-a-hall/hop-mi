/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.phalanxdev.hop;

import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Node;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for generating step metadata for Step's with options that are simple bean properties and
 * are annotated with the SimpleStepOption annotation.
 *
 * @author Mark Hall
 * @version $Revision: $
 */
public class MetaHelper {

  protected static List<PropertyDescriptor> getPropertyStuff( Object target ) throws IntrospectionException {
    List<PropertyDescriptor> stuff = new ArrayList<>();

    BeanInfo bi = Introspector.getBeanInfo( target.getClass() );
    PropertyDescriptor[] props = bi.getPropertyDescriptors();

    for ( PropertyDescriptor p : props ) {
      Method getter = p.getReadMethod();
      Method setter = p.getWriteMethod();
      if ( getter == null || setter == null ) {
        continue;
      }

      SimpleStepOption opt = getter.getAnnotation( SimpleStepOption.class );
      if ( opt == null ) {
        opt = setter.getAnnotation( SimpleStepOption.class );
      }

      if ( opt == null ) {
        continue;
      }
      stuff.add( p );
    }

    return stuff;
  }

  public static StringBuilder getXMLForTarget( Object target )
      throws IntrospectionException, InvocationTargetException, IllegalAccessException {
    List<PropertyDescriptor> props = getPropertyStuff( target );
    StringBuilder builder = new StringBuilder();

    for ( PropertyDescriptor p : props ) {
      Method getter = p.getReadMethod();
      String name = p.getDisplayName();

      Object value = getter.invoke( target );
      if ( value instanceof String ) {
        builder.append( XmlHandler.addTagValue( name, (String) value ) );
      } else if ( value instanceof Integer ) {
        builder.append( XmlHandler.addTagValue( name, (Integer) value ) );
      } else if ( value instanceof Long ) {
        builder.append( XmlHandler.addTagValue( name, (Long) value ) );
      } else if ( value instanceof Double ) {
        builder.append( XmlHandler.addTagValue( name, (Double) value ) );
      } else if ( value instanceof Float ) {
        builder.append( XmlHandler.addTagValue( name, (Float) value ) );
      } else if ( value instanceof Boolean ) {
        builder.append( XmlHandler.addTagValue( name, (Boolean) value ) );
      }
    }

    return builder;
  }

  public static void loadXMLForTarget( Node stepnode, Object target )
      throws HopXmlException, IntrospectionException, InvocationTargetException, IllegalAccessException {
    List<PropertyDescriptor> props = getPropertyStuff( target );

    for ( PropertyDescriptor p : props ) {
      Method setter = p.getWriteMethod();
      Method getter = p.getReadMethod();
      String name = p.getDisplayName();
      Object forValueType = getter.invoke( target );

      String toSet = XmlHandler.getTagValue( stepnode, name );

      if ( forValueType instanceof String ) {
        setter.invoke( target, toSet );
      } else if ( forValueType instanceof Integer ) {
        setter.invoke( target, new Integer( toSet.toString() ) );
      } else if ( forValueType instanceof Long ) {
        setter.invoke( target, new Long( toSet.toString() ) );
      } else if ( forValueType instanceof Double ) {
        setter.invoke( target, new Double( toSet.toString() ) );
      } else if ( forValueType instanceof Float ) {
        setter.invoke( target, new Float( toSet.toString() ) );
      } else if ( forValueType instanceof Boolean ) {
        setter.invoke( target, toSet.toString().equalsIgnoreCase( "Y" ) );
      }
    }
  }
}
