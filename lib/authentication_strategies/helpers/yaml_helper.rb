module AuthenticationStrategies::Helpers
  # Set of helpers for working with YAML
  module YamlHelper
    # Helps with reading YAML files with ERB snippets
    def read_yaml(path)
      begin
        raise "File does not exist!" unless File.exists?(path)
        YAML.load(ERB.new(File.read(path)).result)
      rescue Exception => err
        raise Errors::ConfigurationParsingError,
              "Failed to parse a YAML file! [#{path}]: #{err.message}"
      end
    end
  end
end
