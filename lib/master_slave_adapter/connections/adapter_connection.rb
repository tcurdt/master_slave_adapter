module MasterSlaveAdapter
  module Connections
    class AdapterConnection < ActiveRecord::ConnectionAdapters::AbstractAdapter

      # def add_timestamps(*args)
      # end
      # 
      # def remove_timestamps(*args)
      # end
      # 
      # def add_column_options!(*args)
      # end
      # 
      # def type_to_sql(*args)
      # end
      # 
      # def assume_migrated_upto_version(*args)
      # end
      # 
      # def add_order_by_for_association_limiting!(*args)
      # end
      # 
      # def native_database_types(*args)
      # end
      # 
      # def table_alias_for(*args)
      # end

      def execute(*args)
      end

      def select_one(*args)
      end

      def select_all(*args)
      end

      def select_value(*args)
      end

      def select_values(*args)
      end

      def select_rows(*args)
      end

      def update(*args)
      end

      # def verify!
      # end
      # 
      # def disconnect!
      # end
      # 
      # def run_callbacks(*args)
      # end

      # def connect_to_master
      #   @master_connection ||= ActiveRecord::Base.send( "#{self.master_config[:adapter]}_connection", self.master_config )
      # end
      # 
      # def connect_to_slave
      #   @slave_connection ||= ActiveRecord::Base.send( "#{self.slave_config[:adapter]}_connection", self.slave_config)
      # end
    end
  end
end
