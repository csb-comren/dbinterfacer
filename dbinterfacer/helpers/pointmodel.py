from decimal import Decimal
from datetime import datetime

class Point_Model():
    """
    A class for defining, generating and validating points.
    Points are regular dictionaries. All field names should be strings only.
    To add additional fields to a point prefix the name with `pr_` ex. p['pr_var'] = 42
    All models have fields ('time', datetime),('longitude', Decimal), ('latitude', Decimal).
    More can be added with the constructor or add_field()
    Field names should match the table columns
    """

    DEFAULT_FIELDS =[('time', datetime),('longitude', Decimal), ('latitude', Decimal)]

    def __init__(self, additional_fields=None):
        """
        Constructs the model.
        Takes in an optional array of tuples ('field_name', field_type)
        """
        self.set_model(self.DEFAULT_FIELDS)
        if additional_fields is not None:
            for tuple in additional_fields:
                self.add_field(tuple)


    def generate_point(self):
        """
        returns a dictionary full of None based on the model
        """
        return dict(self.model)


    def validate(self, point):
        """
        Validates a point. True if all fields in model are in point
        and all non pr_ fields in point are in model and if all the
        field are None of the corresponding type
        """
        required_fields = list(self.model)
        for field in point:
            # don't check private fields
            if field.startswith('pr_'):
                continue

            if field not in required_fields or len(required_fields) == 0:
                return False
            required_fields.remove(field)

            value = point[field]
            if value == None:
                continue
            if isinstance(value, self.types[field]) == False:
                return False

        if len(required_fields) > 0:
            return False
        return True


    def set_model(self, fields):
        """
        Sets the model.
        Takes in an array of tuples ('field_name', field_type)
        """
        self.model = {}
        self.types = {}
        for tuple in fields:
            self.add_field(tuple)

    def add_field(self, tuple):
        """
        Adds a field to the model
        :input: a tuple of ('name', type)
        """
        (name, type) = tuple
        self.model[name] = None
        self.types[name] = type
