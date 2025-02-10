import pystac


class StacRepositoryExtension():

    processor_property = "stac-repository:processor"
    processor_version_property = "stac-repository:version:processor"
    product_version_property = "stac-repository:version:product"

    @classmethod
    def implement(
            cls,
            stac_object: pystac.STACObject,
            *,
            processor_id: str,
            processor_version: str,
            product_version: str
    ):
        if isinstance(stac_object, pystac.Item):
            stac_object.properties[cls.processor_property] = processor_id
            stac_object.properties[cls.processor_version_property] = processor_version
            stac_object.properties[cls.product_version_property] = product_version
        elif isinstance(stac_object, pystac.Catalog):
            stac_object.extra_fields[cls.processor_property] = processor_id
            stac_object.extra_fields[cls.processor_version_property] = processor_version
            stac_object.extra_fields[cls.product_version_property] = product_version
        else:
            raise ValueError

    @classmethod
    def implements(
            cls,
            stac_object: pystac.STACObject,
    ) -> bool:
        if isinstance(stac_object, pystac.Item):
            return all(
                [
                    cls.processor_property in stac_object.properties,
                    cls.processor_version_property in stac_object.properties,
                    cls.product_version_property in stac_object.properties
                ]
            )
        elif isinstance(stac_object, pystac.Catalog):
            return all(
                [
                    cls.processor_property in stac_object.extra_fields,
                    cls.processor_version_property in stac_object.extra_fields,
                    cls.product_version_property in stac_object.extra_fields
                ]
            )
        else:
            return False

    @classmethod
    def get_processor(cls, stac_object: pystac.STACObject) -> str | None:
        if isinstance(stac_object, pystac.Item):
            return stac_object.properties.get(cls.processor_property)
        elif isinstance(stac_object, pystac.Catalog):
            return stac_object.extra_fields.get(cls.processor_property)
        else:
            return None

    @classmethod
    def get_processor_version(cls, stac_object: pystac.STACObject) -> str | None:
        if isinstance(stac_object, pystac.Item):
            return stac_object.properties.get(cls.processor_version_property)
        elif isinstance(stac_object, pystac.Catalog):
            return stac_object.extra_fields.get(cls.processor_version_property)
        else:
            return None

    @classmethod
    def get_product_version(cls, stac_object: pystac.STACObject) -> str | None:
        if isinstance(stac_object, pystac.Item):
            return stac_object.properties.get(cls.product_version_property)
        elif isinstance(stac_object, pystac.Catalog):
            return stac_object.extra_fields.get(cls.product_version_property)
        else:
            return None
