class RFBRouter:
    def db_for_read(self, model, **hints):
        if self.is_rfb(model):
            return "rfb"

        return None

    def db_for_write(self, model, **hints):
        if self.is_rfb(model):
            return "rfb"

        return None

    def allow_relation(self, obj1, obj2, **hints):
        is_rfb_1 = self.is_rfb(obj1.__class__)
        is_rfb_2 = self.is_rfb(obj2.__class__)

        if is_rfb_1 and is_rfb_2:
            return True
        if not is_rfb_1 and not is_rfb_2:
            return True

        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if db == "rfb":
            return False

        return None

    @staticmethod
    def is_rfb(model):
        return getattr(model, "rfb", False)
