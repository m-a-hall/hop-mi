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

package org.phalanxdev.hop.ui.pipeline.pmi;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.widgets.Shell;
import org.phalanxdev.hop.pipeline.transforms.pmi.BaseSupervisedPMIMeta;
import org.phalanxdev.hop.pipeline.transforms.pmi.SupervisedEvaluatorMeta;

/**
 * Modern dialog for the Supervised Evaluator transform using GuiCompositeWidgets.
 *
 * @author Mark Hall (mhall{[at]}waikato.ac.nz)
 */
public class SupervisedEvaluatorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BaseSupervisedPMIMeta.PKG;

  private final SupervisedEvaluatorMeta input;
  private GuiCompositeWidgets widgets;

  public SupervisedEvaluatorDialog(
      Shell parent,
      IVariables variables,
      SupervisedEvaluatorMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  public SupervisedEvaluatorDialog(
      Shell parent,
      IVariables variables,
      BaseTransformMeta baseTransformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, baseTransformMeta, pipelineMeta, transformName);
    this.input = (SupervisedEvaluatorMeta) baseTransformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BasePMIStepDialog.Shell.Title", "Supervised evaluator"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            SupervisedEvaluatorMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    widgets.getWidgetsContents(input, SupervisedEvaluatorMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
