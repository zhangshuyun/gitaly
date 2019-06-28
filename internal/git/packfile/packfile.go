package packfile

type PackfileOrder []*Object

func (po PackfileOrder) Len() int           { return len(po) }
func (po PackfileOrder) Less(i, j int) bool { return po[i].Offset < po[j].Offset }
func (po PackfileOrder) Swap(i, j int)      { po[i], po[j] = po[j], po[i] }

type IndexOrder []*Object

func (ido IndexOrder) Len() int           { return len(ido) }
func (ido IndexOrder) Less(i, j int) bool { return ido[i].OID < ido[j].OID }
func (ido IndexOrder) Swap(i, j int)      { ido[i], ido[j] = ido[j], ido[i] }
