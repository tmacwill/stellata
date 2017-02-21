import stellata.relation

class BelongsTo(stellata.relation.Relation):
    def child_id(self):
        return super().child_id()

    def parent_id(self):
        if self.id_lambda:
            return self.id_lambda()

        return super().parent_id()

class HasMany(stellata.relation.Relation):
    def child_id(self):
        if self.id_lambda:
            return self.id_lambda()

        return super().child_id()

    def parent_id(self):
        return super().parent_id()

class HasOne(stellata.relation.Relation):
    def child_id(self):
        if self.id_lambda:
            return self.id_lambda()

        return super().child_id()

    def parent_id(self):
        return super().parent_id()
