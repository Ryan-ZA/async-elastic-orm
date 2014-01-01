package com.rc.gds;

class EmbeddedEntity extends Entity {

	private static final long serialVersionUID = 1L;

	public EmbeddedEntity(String classKind, String specialID) {
		super(classKind, specialID);
	}

	public EmbeddedEntity(String classKind) {
		super(classKind);
	}

	public EmbeddedEntity() {
	}

	public void setPropertiesFrom(Entity entity) {
		classKind = entity.classKind;
		dbObject = entity.dbObject;
	}

}
