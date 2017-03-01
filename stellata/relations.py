import stellata.relation

class BelongsTo(stellata.relation.Relation):
    def child(self):
        return self._parent()

    def parent(self):
        return self.foreign_key_lambda()

class HasMany(stellata.relation.Relation):
    def child(self):
        return self.foreign_key_lambda()

    def parent(self):
        return self._parent()

class HasOne(stellata.relation.Relation):
    def child(self):
        return self.foreign_key_lambda()

    def parent(self):
        return self._parent()
