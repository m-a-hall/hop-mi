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

package org.phalanxdev.hop.pipeline.transforms.pmi;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.phalanxdev.hop.ui.pipeline.pmi.SupervisedEvaluatorDialog;
import weka.core.Attribute;

import java.util.Arrays;

/**
 * Transform that computes supervised evaluation metrics from incoming ground truth class values and predicted
 * class values (as produced as output from a machine learning scheme). Can handle both numeric and nominal classes.
 * When the class is nominal, it is assumed that the predicted values are in the form of a probability distribution for
 * each row.
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 */
@Getter
@Setter
@Transform(
    id = "SupervisedEvaluator",
    image = "WEKAS.svg",
    name = "Supervised Evaluator",
    description = "Compute supervised evaluation metrics for incoming row data that contains predictions from a learning scheme",
    categoryDescription = "PMI"
)
@GuiPlugin
public class SupervisedEvaluatorMeta extends BaseTransformMeta<SupervisedEvaluator, SupervisedEvaluatorData> {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "SUPERVISED_EVALUATOR_DIALOG_OPTIONS";
  public static final String WIDGET_CLASS_NAME = "className";
  public static final String WIDGET_OUTPUT_IR_STATS = "outputIRStats";
  public static final String WIDGET_OUTPUT_AUC = "outputAUC";

  @GuiWidgetElement(
      id = WIDGET_CLASS_NAME,
      order = "0100",
      type = GuiElementType.TEXT,
      label = "SupervisedEvaluator.ClassDropDown.Label",
      toolTip = "SupervisedEvaluator.ClassDropDown.ToolTip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Options"
  )
  @HopMetadataProperty(key = "className")
  private String className = "";

  @GuiWidgetElement(
      id = WIDGET_OUTPUT_IR_STATS,
      order = "0200",
      type = GuiElementType.CHECKBOX,
      label = "SupervisedEvaluator.OutputIRStats.Label",
      toolTip = "SupervisedEvaluator.OutputIRStats.ToolTip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Options"
  )
  @HopMetadataProperty(key = "outputIRStats")
  private boolean outputIRStats;

  @GuiWidgetElement(
      id = WIDGET_OUTPUT_AUC,
      order = "0300",
      type = GuiElementType.CHECKBOX,
      label = "SupervisedEvaluator.OutputAUCStats.Label",
      toolTip = "SupervisedEvaluator.OutputAUCStats.ToolTip",
      parentId = GUI_PLUGIN_ELEMENT_PARENT_ID,
      groupType = GuiWidgetGroupType.BOXES,
      group = "Options"
  )
  @HopMetadataProperty(key = "outputAUC")
  private boolean outputAUC;

  public SupervisedEvaluatorMeta() {
    super();
  }

  @Override
  public void setDefault() {
    className = "";
    outputIRStats = false;
    outputAUC = false;
  }

  public boolean isOutputIRStats() {
    return outputIRStats;
  }

  public boolean getOutputIRStats() {
    return outputIRStats;
  }

  public boolean isOutputAUC() {
    return outputAUC;
  }

  public boolean getOutputAUC() {
    return outputAUC;
  }

  @Override
  public String getDialogClassName() {
    return SupervisedEvaluatorDialog.class.getName();
  }

  protected static Attribute createClassAttribute( String className, String nominalVals ) {
    Attribute classA = null;
    if ( !org.apache.hop.core.util.Utils.isEmpty( nominalVals ) ) {
      String[] labels = nominalVals.split( "," );
      for ( int i = 0; i < labels.length; i++ ) {
        labels[i] = labels[i].trim();
      }
      classA = new Attribute( className, Arrays.asList( labels ) );
    } else {
      // assume numeric class
      classA = new Attribute( className );
    }
    return classA;
  }

  @Override
  public void getFields( IRowMeta rowMeta, String stepName, IRowMeta[] info, TransformMeta nextTransform,
      IVariables space, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    if ( rowMeta != null && rowMeta.size() > 0 && !org.apache.hop.core.util.Utils.isEmpty( getClassName() ) ) {
      String className = space.resolve( getClassName() );
      try {
        GeneralSupervisedEvaluatorUtil eval = new GeneralSupervisedEvaluatorUtil( rowMeta, className );
        eval.getOutputFields( rowMeta, outputIRStats, outputAUC );
      } catch ( HopException e ) {
        throw new HopTransformException( e );
      }
    } else if ( rowMeta != null ) {
      rowMeta.clear();
    }
  }
}
