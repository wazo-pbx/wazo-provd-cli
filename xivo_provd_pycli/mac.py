# -*- coding: utf-8 -*-

# Copyright (C) 2010-2015 Avencall
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

import re

_MAC_ADDR = re.compile(ur'^[\da-fA-F]{1,2}([:-]?)(?:[\da-fA-F]{1,2}\1){4}[\da-fA-F]{1,2}$')


def _to_mac(mac_string):
    m = _MAC_ADDR.match(mac_string)
    if not m:
        raise ValueError('invalid MAC string')
    sep = m.group(1)
    if not sep:
        # no separator - length must be equal to 12 in this case
        if len(mac_string) != 12:
            raise ValueError('invalid MAC string')
        return ''.join(chr(int(mac_string[i:i + 2], 16)) for i in xrange(0, 12, 2))
    else:
        tokens = mac_string.split(sep)
        return ''.join(chr(int(token, 16)) for token in tokens)


def _from_mac(packed_mac, separator=u':', uppercase=False):
    if len(packed_mac) != 6:
        raise ValueError('invalid packed MAC')
    if uppercase:
        fmt = u'%02X'
    else:
        fmt = u'%02x'
    return separator.join(fmt % ord(e) for e in packed_mac)


def norm_mac(mac_string):
    return _from_mac(_to_mac(mac_string))
