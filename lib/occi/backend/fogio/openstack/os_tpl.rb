module OCCI
  module Backend
    class Fogio
      module Openstack
        attr_accessor :model

        class OsTpl
          def initialize(model, backend)
            @model = model
            @backend = backend
          end

          def register_all(client)
            client.list_images.body['images'].each do |image|
              parse_backend_object client, image
            end
          end

          private
          def parse_backend_object(client, backend_object)
            related = %w|http://schemas.ogf.org/occi/infrastructure#os_tpl|
            term = backend_object['id'].downcase.chomp.gsub(/\W/, '_')
            # TODO: implement correct schema for service provider
            scheme = @backend.scheme + "/occi/infrastructure/os_tpl#"
            title = backend_object['name']
            mixin = OCCI::Core::Mixin.new(scheme, term, title, nil, related)
            mixin.attributes.org!.openstack!.os_tpl!.id = OCCI::Core::AttributeProperties.new({:Default => backend_object['id']})
            @model.register(mixin)
          end
        end
      end
    end
  end
end