class CalcEngine(object):
    def __init__(self, name, url,  methods=[]):
        self._name = name
        self._methods = methods
        self._url = url
        
     
    def basic_ret