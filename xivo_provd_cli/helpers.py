# -*- coding: utf-8 -*-

# Copyright (C) 2011-2016 Avencall
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
import sys as _sys


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
    print 'Nb of devices: %s' % _devices.count()
    print 'Nb of configs: %s' % _configs.count()
    print 'Nb of installed plugins: %s' % _plugins.count_installed()


def detailed_system_info():
    """Print various system information."""
    print 'Nb of devices: %s' % _devices.count()
    for device in _devices.find(fields=[u'id']):
        print '    %s' % device[u'id']
    print 'Nb of configs: %s' % _configs.count()
    for config in _configs.find(fields=[u'id']):
        print '    %s' % config[u'id']
    print 'Nb of installed plugins: %s' % _plugins.count_installed()
    for plugin in _plugins.installed():
        print '    %s' % plugin


def used_plugins():
    """Return the list of plugins used by devices."""
    s = set(map(_itemgetter_default(u'plugin', None), _devices.find(fields=[u'plugin'])))
    # None might be present if at least one device has no plugins
    s.discard(None)
    return sorted(s)


def installed_plugins():
    """Return the list of all installed plugins."""
    return sorted(_plugins.installed().iterkeys())


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


def mass_update_devices_plugin(old_plugin, new_plugin, synchronize=False):
    """Update all devices using plugin old_plugin to plugin new_plugin, and
    optionally synchronize the affected devices.
    
    """
    if not isinstance(old_plugin, basestring):
        raise ValueError(old_plugin)
    if not isinstance(new_plugin, basestring):
        raise ValueError(new_plugin)

    installed_plugins = set(_plugins.installed())
    if not _are_plugins_installed([old_plugin, new_plugin], installed_plugins):
        answer = raw_input('Do you want to proceed anyway? [Y/n] ')
        if answer and answer not in ('Y', 'y'):
            return

    for device in _devices.find({u'plugin': old_plugin}):
        device[u'plugin'] = new_plugin
        print 'Updating device %s' % device[u'id']
        _devices.update(device)
        if synchronize:
            print 'Synchronizing device %s' % device[u'id']
            _devices.synchronize(device)
        print


def _are_plugins_installed(plugins, installed_plugins):
    result = True
    for plugin in plugins:
        if not _is_plugin_installed(plugin, installed_plugins):
            result = False
    return result


def _is_plugin_installed(plugin, installed_plugins):
    if plugin in installed_plugins:
        return True

    print >>_sys.stderr, 'Error: plugin %s is not installed' % plugin
    return False


def mass_synchronize():
    """Synchronize all devices."""
    for device in _devices.find(fields=[u'id']):
        print 'Synchronizing device %s' % device[u'id']
        _devices.synchronize(device)
        print


def remove_transient_configs():
    """Remove any unused transient config. Mostly useful for debugging purpose."""
    n = 0
    for config in map(_operator.itemgetter(u'id'), _configs.find({u'transient': True}, fields=[u'id'])):
        if not _devices.find({u'config': config}, fields=[u'id']):
            print 'Removing config %s' % config
            _configs.remove(config)
            n += 1
    print '%d unused transient configs have been removed' % n
