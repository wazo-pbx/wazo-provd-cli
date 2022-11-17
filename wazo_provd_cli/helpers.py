# Copyright 2011-2022 The Wazo Authors  (see the AUTHORS file)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

"""Various helpers functions to be used in the CLI."""


# importing <module> as _<module> so that import are not autocompleted in the CLI
import operator as _operator
import sys


def _init_module(configs, devices, plugins):
    # MUST be called from another module before the function in this module
    # are made available in the CLI
    global _configs
    global _devices
    global _plugins
    _configs = configs
    _devices = devices
    _plugins = plugins


def _itemgetter_default(item, default):
    def aux(obj):
        try:
            return obj[item]
        except LookupError:
            return default

    return aux


def system_info():
    """Print various system information."""
    print(f'Nb of devices: {_devices.count()}')
    print(f'Nb of configs: {_configs.count()}')
    print(f'Nb of installed plugins: {_plugins.count_installed()}')


def detailed_system_info():
    """Print various system information."""
    print(f'Nb of devices: {_devices.count()}')
    for device in _devices.find(fields=['id']):
        print(f'    {device["id"]}')
    print(f'Nb of configs: {_configs.count()}')
    for config in _configs.find(fields=['id']):
        print(f'    {config["id"]}')
    print(f'Nb of installed plugins: {_plugins.count_installed()}')
    for plugin in _plugins.installed():
        print(f'    {plugin}')


def used_plugins():
    """Return the list of plugins used by devices."""
    s = set(map(_itemgetter_default('plugin', None), _devices.find(fields=['plugin'])))
    # None might be present if at least one device has no plugins
    s.discard(None)
    return sorted(s)


def installed_plugins():
    """Return the list of all installed plugins."""
    return sorted(_plugins.installed().keys())


def unused_plugins():
    """Return the list of unused plugins, i.e. installed plugins that no
    devices are using.

    """
    return sorted(set(installed_plugins()) - set(used_plugins()))


def missing_plugins():
    """Return the list of missing plugins, i.e. non-installed plugins that
    are used by at least one device.

    """
    return sorted(set(used_plugins()) - set(installed_plugins()))


def mass_update_devices_plugin(
    old_plugin, new_plugin, synchronize=False, recurse=False
):
    """Update all devices using plugin old_plugin to plugin new_plugin, and
    optionally synchronize the affected devices.

    """
    if not isinstance(old_plugin, str):
        raise ValueError(old_plugin)
    if not isinstance(new_plugin, str):
        raise ValueError(new_plugin)

    installed_plugins = set(_plugins.installed())
    if not _are_plugins_installed([old_plugin, new_plugin], installed_plugins):
        answer = input('Do you want to proceed anyway? [Y/n] ')
        if answer and answer not in ('Y', 'y'):
            return

    for device in _devices.find({'plugin': old_plugin}, recurse=recurse):
        device['plugin'] = new_plugin
        print(f'Updating device {device["id"]}')
        _devices.update(device)
        if synchronize:
            print(f'Synchronizing device {device["id"]}')
            _devices.synchronize(device)
        print()


def _are_plugins_installed(plugins, installed_plugins):
    result = True
    for plugin in plugins:
        if not _is_plugin_installed(plugin, installed_plugins):
            result = False
    return result


def _is_plugin_installed(plugin, installed_plugins):
    if plugin in installed_plugins:
        return True

    print(f'Error: plugin {plugin} is not installed', file=sys.stderr)
    return False


def mass_synchronize(recurse=False):
    """Synchronize all devices."""
    for device in _devices.find(fields=['id'], recurse=recurse):
        print(f'Synchronizing device {device["id"]}')
        _devices.synchronize(device)
        print()


def remove_transient_configs():
    """Remove any unused transient config. Mostly useful for debugging purpose."""
    n = 0
    for config in map(
        _operator.itemgetter('id'), _configs.find({'transient': True}, fields=['id'])
    ):
        if not _devices.find({'config': config}, fields=['id']):
            print(f'Removing config {config}')
            _configs.remove(config)
            n += 1
    print(f'{n:d} unused transient configs have been removed')
