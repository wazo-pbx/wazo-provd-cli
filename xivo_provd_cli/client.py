# -*- coding: utf-8 -*-

# Copyright 2011-2019 The Wazo Authors  (see the AUTHORS file)
# SPDX-License-Identifier: GPL-3.0+

from copy import deepcopy
from time import sleep
from sys import stdout
from itertools import chain
from wazo_provd_client.operation import OIP_SUCCESS, OIP_FAIL, OIP_WAITING, \
    OIP_PROGRESS, BaseOperation
from xivo_provd_cli.mac import norm_mac
from wazo_provd_client import Client as ProvdClient


class _Options(object):
    def __init__(self):
        self.search_description = True
        self.search_case_sensitive = False
        self.op_progress = True
        self.op_async = False
        self.oip_update_interval = 1.0


OPTIONS = _Options()


_FMT_STATE_MAP = {
    OIP_WAITING: 'waiting...',
    OIP_PROGRESS: 'in progress...',
    OIP_FAIL: 'failed.',
    OIP_SUCCESS: 'done.'
}


def _format_oip(oip):
    # format the oip
    dict_ = {}
    if oip.label:
        dict_['label'] = "'%s'" % oip.label
    else:
        dict_['label'] = 'operation'
    dict_['state'] = _FMT_STATE_MAP[oip.state]
    if oip.current is not None:
        if oip.end:
            dict_['xy'] = '%s/%s' % (oip.current, oip.end)
        else:
            dict_['xy'] = oip.current
    else:
        dict_['xy'] = ''
    return '%(label)s %(state)s %(xy)s' % dict_


def _format_oip_line(oip, tree_pos, sw=4):
    # format oip on a line, i.e. format the oip with correct leading indent.
    # Note: tree_pos is only used to compute indent
    indent = ' ' * (sw * len(tree_pos))
    fmted_oip = _format_oip(oip)
    return indent + fmted_oip


def _build_write_table(oip):
    # a write table is a list where item are position specification
    # - a position specification is a tuple (tree position, completed)
    #   - a tree position is a tuple that is used to reach the oip in an oip tree
    #   - completed is true when the item represent an completed operation (i.e.
    #     an operation in state fail or success), else false
    def aux(cur_oip, cur_pos):
        sub_table = []
        for pos_suffix, oip in enumerate(cur_oip.sub_oips):
            pos = cur_pos + (pos_suffix,)
            sub_table.extend(aux(oip, pos))
        return list(chain([(cur_pos, False)], sub_table, [(cur_pos, True)]))
    return aux(oip, ())


def _retrieve_oip(top_oip, tree_pos):
    # return the oip at tree position tree_pos
    oip = top_oip
    for pos in tree_pos:
        oip = oip.sub_oips[pos]
    return oip


def _write_oip_info(top_oip, init_pos_spec, cur_pos_spec, fobj):
    # This function write the the line between init_pos_spec and cur_pos_spec.
    # If they are the same, rewrite the line at init_pos_spec
    # 1. create a flat list of operation to visit
    write_table = _build_write_table(top_oip)
    # 2. find the one will visit in this call
    init_idx = write_table.index(init_pos_spec)
    cur_idx = write_table.index(cur_pos_spec)
    # 3. write the lines...
    pos_specs = set(write_table[init_idx:cur_idx + 1])
    for idx in xrange(init_idx, cur_idx + 1):
        tree_pos, completed = write_table[idx]
        oip = _retrieve_oip(top_oip, tree_pos)
        if not completed and (tree_pos, True) in pos_specs:
            if idx == init_idx and idx != 0:
                # just skip it
                fobj.write('\n')
                continue
            else:
                # we need to rewrite the oip
                oip = BaseOperation(oip.label, OIP_PROGRESS)
        line = _format_oip_line(oip, tree_pos)
        fobj.write('\r' + line)
        if idx != cur_idx:
            fobj.write('\n')
        fobj.flush()


def _find_active_oip(top_oip):
    # return the position specification of the active oip
    def aux(cur_oip, cur_tree_pos):
        if cur_oip.state in [OIP_SUCCESS, OIP_FAIL]:
            return (cur_tree_pos, True)
        for pos_suffix, oip in enumerate(cur_oip.sub_oips):
            if oip.state == OIP_PROGRESS:
                return aux(oip, cur_tree_pos + (pos_suffix,))
        else:
            # oip.state is either waiting or progress
            return (cur_tree_pos, False)
    return aux(top_oip, ())


def _display_operation_in_progress(client_oip):
    init_pos_spec = ((), False)
    while True:
        client_oip.update()
        cur_pos_spec = _find_active_oip(client_oip)
        _write_oip_info(client_oip, init_pos_spec, cur_pos_spec, stdout)
        if client_oip.state in [OIP_SUCCESS, OIP_FAIL]:
            # operation completed
            assert cur_pos_spec == ((), True)
            break
        else:
            init_pos_spec = cur_pos_spec
            sleep(OPTIONS.oip_update_interval)
    print


def _nodisplay_operation_in_progress(client_oip):
    while True:
        if client_oip.state in [OIP_SUCCESS, OIP_FAIL]:
            break
        else:
            sleep(OPTIONS.oip_update_interval)
    print _format_oip_line(client_oip, ())


def _search_in_pkgs_gen(pkgs, search):
    # define search package predicate
    if OPTIONS.search_description:
        def search_pkg_pred(pkg_idpkg_name, pkg):
            if search_pred(pkg_id):
                return True
            else:
                if u'description' in pkg:
                    return search_pred(pkg[u'description'])
            return False
    else:
        def search_pkg_pred(pkg_id, pkg):
            return search_pred(pkg_id)
    # define search predicate
    if OPTIONS.search_case_sensitive:
        def search_pred(value):
            return search in value
    else:
        search = search.lower()

        def search_pred(value):
            return search in value.lower()
    for pkg_id, pkg in pkgs.iteritems():
        if search_pkg_pred(pkg_id, pkg):
            yield pkg_id, pkg


def _search_in_pkgs(pkgs, search):
    # return only the pkgs that match the search
    if search is None:
        return pkgs
    else:
        return dict(_search_in_pkgs_gen(pkgs, search))


def _get_id(id_or_dict):
    if isinstance(id_or_dict, basestring):
        return id_or_dict
    else:
        return id_or_dict[u'id']


class ProvisioningClient(object):
    def __init__(self, prov_client):
        self._prov_client = prov_client

    @property
    def prov_client(self):
        return self._prov_client

    def configs(self):
        return Configs(self._prov_client.configs)

    def devices(self):
        return Devices(self._prov_client.devices)

    def plugins(self):
        return Plugins(self._prov_client.plugins)

    def parameters(self):
        return Parameters(self._prov_client.params)

    def test_connectivity(self):
        try:
            self.plugins.list_installable()
        except Exception as e:
            print >> sys.stderr, 'Error while connecting to xivo-provd:', e


def _rec_update_dict(base_dict, overlay_dict):
    # update a base dictionary from another dictionary
    for k, v in overlay_dict.iteritems():
        if isinstance(v, dict):
            old_v = base_dict.get(k)
            if isinstance(old_v, dict):
                _rec_update_dict(old_v, v)
            else:
                base_dict[k] = {}
                _rec_update_dict(base_dict[k], v)
        else:
            base_dict[k] = v


def _do_expand_dotted_dict(dotted_dict, result_dict):
    for k, v in dotted_dict.iteritems():
        k_head, sep, k_tail = k.partition('.')
        if sep:
            # dotted key
            if k_head in result_dict:
                cur_result_dict = result_dict[k_head]
                # replace result with empty dict if not dict
                if not isinstance(cur_result_dict, dict):
                    cur_result_dict = {}
                    result_dict[k_head] = cur_result_dict
            else:
                cur_result_dict = {}
                result_dict[k_head] = cur_result_dict
            _do_expand_dotted_dict({k_tail: v}, cur_result_dict)
        else:
            # non dotted key
            if isinstance(v, dict):
                if k in result_dict:
                    cur_result_dict = result_dict[k]
                    if isinstance(cur_result_dict, dict):
                        # merge result
                        _do_expand_dotted_dict(v, cur_result_dict)
                    else:
                        # overwrite result
                        cur_result_dict = {}
                        _do_expand_dotted_dict(v, cur_result_dict)
                        result_dict[k] = cur_result_dict
                else:
                    cur_result_dict = {}
                    _do_expand_dotted_dict(v, cur_result_dict)
                    result_dict[k] = cur_result_dict
            else:
                if k in result_dict:
                    old_v = result_dict[k]
                    # overwrite if not dict
                    if not isinstance(old_v, dict):
                        result_dict[k] = v
                else:
                    result_dict[k] = v


def _expand_dotted_dict(dotted_dict):
    """Expand a "dotted dict" to a regular dict.

    >>> _expand_dotted_dict({'a.b': 'v'})
    {'a': {'b': 'v'}}
    >>> _expand_dotted_dict({'a.b': {'c': 'v', 'd': 'v'}})
    {'a': {'b': {'c': 'v', 'd': 'v'}}}

    Some dotted dicts have undefined expansion, for example:
      {'a.b': 'v1', 'a': {'b': 'v2'}}
      {'a': {'b': {'c': 'v1'}}, 'a.b': {'c': 'v1'}}

    """
    result = {}
    _do_expand_dotted_dict(dotted_dict, result)
    return result


class Configs(object):
    def __init__(self, cfg_mgr):
        self._cfg_mgr = cfg_mgr

    def add(self, dotted_config):
        config = _expand_dotted_dict(dotted_config)
        return self._cfg_mgr.create(config)

    def get(self, id_or_config):
        id = _get_id(id_or_config)
        return self._cfg_mgr.get(id)

    def get_raw(self, id_or_config):
        id = _get_id(id_or_config)
        return self._cfg_mgr.get_raw(id)

    def update(self, dotted_config):
        config = _expand_dotted_dict(dotted_config)
        self._cfg_mgr.update(config)

    def remove(self, id_or_config):
        id = _get_id(id_or_config)
        self._cfg_mgr.delete(id)

    def remove_all(self):
        for config in self._cfg_mgr.list({})['configs']:
            config_id = config[u'id']
            print 'Removing config %s' % config_id
            self._cfg_mgr.delete(config_id)

    def autocreate(self):
        return self._cfg_mgr.autocreate()['id']

    def clone(self, id_or_config, new_id=None):
        old_id = _get_id(id_or_config)
        config = self._cfg_mgr.get(old_id)
        if new_id is not None:
            config[u'id'] = new_id
        return self._cfg_mgr.create(config)

    def find(self, *args, **kwargs):
        return self._cfg_mgr.list(*args, **kwargs)['configs']

    def __getitem__(self, name):
        return Config(name, self._cfg_mgr)

    def count(self):
        return len(self._cfg_mgr.list(fields=[u'id'])['configs'])


class Config(object):
    def __init__(self, id, cfg_mgr):
        self._id = id
        self._cfg_mgr = cfg_mgr

    @property
    def id(self):
        return self._id

    def get(self):
        return self._cfg_mgr.get(self._id)

    def get_raw(self):
        return self._cfg_mgr.get_raw(self._id)

    def set_config(self, dotted_values):
        values = _expand_dotted_dict(dotted_values)
        old_config = self._cfg_mgr.get(self._id)
        new_config = deepcopy(old_config)
        _rec_update_dict(new_config[u'raw_config'], values)
        if new_config != old_config:
            self._cfg_mgr.update(new_config)
        return self

    def unset_config(self, *raw_values):
        old_config = self._cfg_mgr.get(self._id)
        new_config = deepcopy(old_config)
        for raw_value in raw_values:
            keys = raw_value.split('.')
            cur_dict = new_config[u'raw_config']
            for key in keys[:-1]:
                if key in cur_dict and isinstance(cur_dict[key], dict):
                    cur_dict = cur_dict[key]
                else:
                    break
            else:
                key = keys[-1]
                if key in cur_dict:
                    del cur_dict[key]
        if old_config != new_config:
            self._cfg_mgr.update(new_config)
        return self

    def set_parents(self, *parents):
        config = self._cfg_mgr.get(self._id)
        config[u'parent_ids'] = list(parents)
        self._cfg_mgr.update(config)


class Devices(object):
    def __init__(self, dev_mgr):
        self._dev_mgr = dev_mgr

    def add(self, device):
        return self._dev_mgr.create(device)

    def get(self, id_or_device):
        # return a device as a dictionary
        # see __getitem__ to retrieve it as an object
        id = _get_id(id_or_device)
        return self._dev_mgr.get(id)

    def update(self, device):
        self._dev_mgr.update(device)

    def remove(self, id_or_device):
        id = _get_id(id_or_device)
        self._dev_mgr.delete(id)

    def remove_all(self):
        for device in self._dev_mgr.list({}):
            device_id = device[u'id']
            print 'Removing device %s' % device_id
            self._dev_mgr.delete(device_id)

    def reconfigure(self, id_or_device):
        id = _get_id(id_or_device)
        self._dev_mgr.reconfigure(id)

    def synchronize(self, id_or_device):
        id = _get_id(id_or_device)
        client_oip = self._dev_mgr.synchronize(id)
        try:
            _display_operation_in_progress(client_oip)
        finally:
            client_oip.delete()

    def find(self, *args, **kwargs):
        return self._dev_mgr.list(*args, **kwargs)['devices']

    def __getitem__(self, name):
        return Device(name, self._dev_mgr)

    def count(self):
        return len(self._dev_mgr.list(fields=[u'id']))

    def using_plugin(self, plugin_id):
        return self._new_device_group_from_selector({u'plugin': plugin_id})

    def _new_device_group_from_selector(self, selector):
        devices = self._dev_mgr.list(selector, fields=[u'id'])
        device_ids = [device[u'id'] for device in devices]
        return DeviceGroup(self._dev_mgr, device_ids)

    def using_mac(self, mac):
        normalized_mac = norm_mac(mac)
        return self._new_device_group_from_selector({u'mac': normalized_mac})


class DeviceGroup(object):
    def __init__(self, dev_mgr, device_ids):
        self._dev_mgr = dev_mgr
        self._device_ids = device_ids

    def reconfigure(self):
        for device_id in self._device_ids:
            print 'Reconfiguring device %s' % device_id
            self._dev_mgr.reconfigure(device_id)

    def synchronize(self):
        for device_id in self._device_ids:
            print 'Synchronizing device %s' % device_id
            client_oip = self._dev_mgr.synchronize(device_id)
            try:
                _display_operation_in_progress(client_oip)
            finally:
                client_oip.delete()


class Device(object):
    # handy way to do simple modification to a device
    def __init__(self, id, dev_mgr):
        self._id = id
        self._dev_mgr = dev_mgr

    @property
    def id(self):
        return self._id

    def get(self):
        return self._dev_mgr.get(self._id)

    def set(self, values):
        old_device = self._dev_mgr.get(self._id)
        new_device = deepcopy(old_device)
        for k, v in values.iteritems():
            new_device[k] = v
        if new_device != old_device:
            self._dev_mgr.update(new_device)
        return self

    def unset(self, *values):
        old_device = self._dev_mgr.get(self._id)
        new_device = deepcopy(old_device)
        for k in values:
            if k in old_device:
                del new_device[k]
        if new_device != old_device:
            self._dev_mgr.update(new_device)
        return self

    def reconfigure(self):
        self._dev_mgr.reconfigure(self._id)
        return self

    def synchronize(self):
        client_oip = self._dev_mgr.synchronize(self._id)
        try:
            _display_operation_in_progress(client_oip)
        finally:
            client_oip.delete()


class Plugins(object):
    def __init__(self, pg_mgr):
        self._pg_mgr = pg_mgr

    def install(self, id):
        client_oip = self._pg_mgr.install(id)
        try:
            _display_operation_in_progress(client_oip)
        finally:
            client_oip.delete()

    def upgrade(self, id):
        client_oip = self._pg_mgr.upgrade(id)
        _display_operation_in_progress(client_oip)
        client_oip.delete()

    def uninstall(self, id):
        self._pg_mgr.uninstall(id)

    def uninstall_all(self):
        pg_ids = sorted(self._pg_mgr.list_installed()['plugins'])
        for pg_id in pg_ids:
            print 'Uninstalling plugin %s' % pg_id
            self._pg_mgr.uninstall(pg_id)

    def reload(self, id):
        """Reload the plugin with the given ID. This is mostly useful for
        debugging purpose.

        """
        self._pg_mgr.reload(id)

    def update(self):
        client_oip = self._pg_mgr.update()
        if OPTIONS.op_async:
            return client_oip
        else:
            try:
                if OPTIONS.op_progress:
                    _display_operation_in_progress(client_oip)
                else:
                    _nodisplay_operation_in_progress(client_oip)
            finally:
                client_oip.delete()

    def installed(self, search=None):
        plugins_installed = self._pg_mgr.list_installed()['pkgs']
        return _search_in_pkgs(plugins_installed, search)

    def installable(self, search=None):
        plugins_installable = self._pg_mgr.list_installable()['pkgs']
        return _search_in_pkgs(plugins_installable, search)

    def __getitem__(self, plugin_id):
        # return the plugin with id id
        return Plugin(self._pg_mgr, plugin_id)

    def count_installed(self):
        return len(self._pg_mgr.list_installed()['pkgs'])


class Parameters(object):
    def __init__(self, config_srv):
        self._config_srv = config_srv

    def infos(self):
        return self._config_srv.list()['params']

    def get(self, key):
        return self._config_srv.get(key)

    def set(self, key, value):
        self._config_srv.update(key, value)

    def unset(self, key):
        # equivalent to set(key, None)
        self._config_srv.update(key, None)


class Plugin(object):
    def __init__(self, client_plugin, plugin_id):
        self._client_plugin = client_plugin
        self._plugin_id = plugin_id

    def install(self, id):
        client_oip = self._client_plugin.install_package(self._plugin_id, id)
        try:
            _display_operation_in_progress(client_oip)
        finally:
            client_oip.delete()

    def install_all(self):
        """Install all the packages available from this plugin."""
        pkg_ids = sorted(self._client_plugin.get_packages_installable(self._plugin_id)['pkgs'])
        for pkg_id in pkg_ids:
            print 'Installing package %s' % pkg_id
            client_oip = self._client_plugin.install_package(self._plugin_id, pkg_id)
            try:
                _display_operation_in_progress(client_oip)
            finally:
                client_oip.delete()
            print

    def upgrade(self, id):
        client_oip = self._client_plugin.upgrade_package(self._plugin_id, id)
        try:
            _display_operation_in_progress(client_oip)
        finally:
            client_oip.delete()

    def uninstall(self, id):
        self._client_plugin.uninstall(id)

    def uninstall_all(self):
        pkg_ids = sorted(self._client_plugin.get_packages_installed(self._plugin_id)['pkgs'])
        for pkg_id in pkg_ids:
            print 'Uninstalling package %s' % pkg_id
            self._client_plugin.uninstall_package(self._plugin_id, pkg_id)

    def installed(self, search=None):
        pkgs = self._client_plugin.get_packages_installed(self._plugin_id)['pkgs']
        return _search_in_pkgs(pkgs, search)

    def installable(self, search=None):
        pkgs = self._client_plugin.get_packages_installable(self._plugin_id)['pkgs']
        return _search_in_pkgs(pkgs, search)


def new_cli_provisioning_client(provd_args):
    prov_client = ProvdClient(**provd_args)
    return ProvisioningClient(prov_client)
