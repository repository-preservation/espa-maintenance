
import os
import sys
import logging
import json

class OrderParameter(object):

    _name = None

    def __init__(self, name):
        self._name = name

    def name(self):
        return self._name

    def value(self):
        raise NotImplementedError("Must implement value()")


class OrderIdParameter(OrderParameter):

    _orderid = None

    def __init__(self, value):
        super(OrderIdParameter, self).__init__('orderid')
        self._orderid = value

    def value(self):
        return str(self._orderid)


class SceneParameter(OrderParameter):

    _scene = None

    def __init__(self, value):
        super(SceneParameter, self).__init__('scene')
        self._scene = value

    def value(self):
        return str(self._scene)


class XmlrcpParameter(OrderParameter):

    _xmlrpc = None

    def __init__(self, value):
        super(XmlrcpParameter, self).__init__('xmlrpc')
        self._xmlrpc = value

    def value(self):
        return str(self._xmlrpc)


class OptionsParameter(OrderParameter):

    _options = None

    def __init__(self, value):
        super(OptionsParameter, self).__init__('options')
        self._options = value

    def value(self):
        return str(self._options)


class OrderParameters(object):

    parameters = list()

    def __init__(self, order_string):
        parms = json.loads(order_string)

        self.parameters = [self._parameter_instance(name, value)
                           for (name, value) in parms.items()]


    def _parameter_instance(self, name, value):

        if name == 'orderid':
            return OrderIdParameter(value)
        elif name == 'scene':
            return SceneParameter(value)
        elif name == 'xmlrpc':
            return XmlrcpParameter(value)
        elif name == 'options':
            return OptionsParameter('tulip.com')
        else:
            message = "[%s] is not a valid parameter" % name
            raise NotImplementedError(message)


    def __iter__(self):
        for item in self.parameters:
            name = item.name()
            if name != 'options':
                yield (name, item.value())


if __name__ == '__main__':
    order = OrderParameters('{ "orderid": "water", "scene": "town", "xmlrpc": "skip_xmlrpc", "options": { "keep_log": true } }')

    print dict(order)
    print json.dumps(dict(order), indent=4, sort_keys=True)



#    parameter_1 = order_parameter_instance('orderid', 'water')
#    parameter_2 = order_parameter_instance('scene', 'town')
#
#    print parameter_1.orderid
#
#    igor = parameter_1.__dict__
#    print igor

    sys.exit()
