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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.phalanxdev.mi.MIEnvironmentInit;

/**
 * Executed at Hop init in order to install (if necessary) and load required Weka packages
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 * @version $Revision: $
 */
@ExtensionPoint(id = "PMIInit", extensionPointId = "HopEnvironmentAfterInit", description = "Ensures PMI plugins are installed and loads Weka packages")
public class PMILifecycleListener
    implements IExtensionPoint<PluginRegistry> {

  public static final String HOP_MI_AUTO_INSTALL_PACKAGES = "HOP_MI_AUTO_INSTALL_PACKAGES";

  @Override
  public void callExtensionPoint(ILogChannel iLogChannel, IVariables variables, PluginRegistry pluginRegistry)
      throws HopException {
    iLogChannel.logBasic("Checking and loading packages for Hop machine intelligence.");

    String autoInstallProp = variables != null ? variables.getVariable(HOP_MI_AUTO_INSTALL_PACKAGES, "Y") : "Y";
    boolean checkPackages = "Y".equalsIgnoreCase(autoInstallProp) || "true".equalsIgnoreCase(autoInstallProp);

    MIEnvironmentInit miEnvironmentInit = new MIEnvironmentInit();
    try {
      miEnvironmentInit.onEnvironmentInit(checkPackages);
    } catch (Throwable ex) {
      iLogChannel.logError("Error initializing Hop machine intelligence environment: " + ex.getMessage(), ex);
    }
  }
}
