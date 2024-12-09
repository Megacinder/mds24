from datetime import date


class Car:
    def __init__(self, brand, age):
        self.brand = brand
        self.age = age

    @classmethod
    def from_production_year(cls, brand, prod_year):
        return cls(brand, date.today().year - prod_year)

    @staticmethod
    def is_warranty_active(age):
        return age < 3

    def info(self):
        print("Car: " + self.brand)
        print("Age: " + str(self.age))
        if self.is_warranty_active(self.age):
            print("Warranty is ACTIVE")
        else:
            print("Warranty is NOT active")


a = Car('aaa', 30)
a.is_warranty_active()

