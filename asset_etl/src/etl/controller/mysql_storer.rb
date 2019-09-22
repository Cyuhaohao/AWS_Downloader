
require_relative '../../packages/common/databese/mysql_client'

module Puzzle
  module AssetETL

    class MysqlStorer

        ASSET_ETL_INSERT_SQL = "
            insert into {table_name}
              (file_path, asset_index, asset_path, player_level, preinstall, platform, version, asset, time)
            values"

        ASSET_ETL_CREATE_TABLE_SQL = "
            CREATE TABLE IF NOT EXISTS {table_name} (
              `file_path` varchar(100) NOT NULL,
              `asset_index` varchar(200) NOT NULL,
              `asset_path` varchar(200) DEFAULT NULL,
              `player_level` varchar(5) DEFAULT NULL,
              `preinstall` varchar(10) DEFAULT NULL,
              `platform` varchar(20) DEFAULT NULL,
              `version` varchar(20) DEFAULT NULL,
              `asset` longtext,
              `time` int(20) DEFAULT NULL)"

        def initialize(options = {})
          @options = options

          @db_single_insert = @options[:mysql][:db_single_insert]

          @table_name = @options[:mysql][:table_name].gsub('{env}', @options[:game_env])

          @mysql = Packages::MysqlClient.new(@options[:mysql][:mysql_config])

          @mysql.connect

          # create table if not exists
          begin
            @mysql.execute(ASSET_ETL_CREATE_TABLE_SQL.gsub('{table_name}', @table_name))
          rescue Mysql2::Error => e
            puts e
          end

          # init insert sql statement
          @statementStr = ASSET_ETL_INSERT_SQL.gsub('{table_name}', @table_name)
          @statementArray = @statementStr
          @dataArray = []
          @prepareArray = []

        end

        def save(data)
          @dataArray.push(data)
          if @dataArray.length == @db_single_insert
            saveArray
          end
        end

        private

        def close()
          @mysql.close if @mysql
        end

        def saveArray()

          for data in @dataArray do
            @statementArray += ' (?, ?, ?, ?, ?, ?, ?, ?, ?),'

            begin

              json_data = data[:json_data]

              file_path = data[:file_path]
              asset_index = data[:asset_index]
              time = data[:record_time] || 0

              asset_path = json_data['assetPath'] || ''
              player_level = json_data['playerLevel'] || ''
              preinstall = json_data['preinstall'] || ''
              platform = json_data['platform'] || ''
              version = json_data['version'] || ''
              asset = json_data.to_json

              @prepareArray.push(file_path)
              @prepareArray.push(asset_index)
              @prepareArray.push(asset_path)
              @prepareArray.push(player_level)
              @prepareArray.push(preinstall)
              @prepareArray.push(platform)
              @prepareArray.push(version)
              @prepareArray.push(asset)
              @prepareArray.push(time)

            rescue Exception => e
                puts e.message + ', data: ' + data.to_json
            end

          end

          if @prepareArray.length == 0
            return
          end

          @statementArray.chop
          @statement = @mysql.prepare(@statementArray.chop)

          begin
            @mysql.stmt_execute(@statement, @prepareArray)
          rescue Mysql2::Error => e
            if e.errno != 1062
              puts e.message + ', data: ' + data.to_json
            end
          end

          @statementArray = @statementStr
          @dataArray = []
          @prepareArray = []
        end

    end

  end
end
