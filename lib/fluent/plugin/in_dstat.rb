require 'fluent/mixin/rewrite_tag_name'

module Fluent
  class DstatInput < Input

    Plugin.register_input('dstat', self)

    def initialize
      super

      require 'csv'
      @first_keys = []
      @second_keys = []
      @data_array = []
      @max_lines = 100
      @last_time = Time.now
    end

    config_param :tag, :string
    config_param :dstat_path, :string, :default => "dstat"
    config_param :option, :string, :default => "-fcdnm"
    config_param :delay, :integer, :default => 1
    config_param :tmp_file, :string, :default => "/tmp/dstat.csv"
    config_param :hostname_command, :string, :default => "hostname"

    include Fluent::Mixin::RewriteTagName

    def configure(conf)
      super

      @command = "#{@dstat_path} #{@option} --output #{@tmp_file} #{@delay} 0"
      @hostname = `#{@hostname_command}`.chomp!
    end

    def start
      touch_or_truncate(@tmp_file)

      @loop = Coolio::Loop.new
      @dw = &method(:receive_lines)
      @dw.attach(@loop)
      @thread = Thread.new(&method(:run))
    end

    def shutdown
      @dw.detach
      @loop.stop
      @thread.join
      File.delete(@tmp_file)
    end

    def run
      begin
        @loop.run
      rescue
        $log.error "unexpected error", :error=>$!.to_s
        $log.error_backtrace
      end
    end

    def restart
      @dw.detach
      touch_or_truncate(@tmp_file)

      @dw = &method(:receive_lines)
      @dw.attach(@loop)
    end

    def touch_or_truncate(file)
      if File.exist?(file)
        File.truncate(file, 0)
      else
        `touch #{file}`
      end
    end

    def receive_lines
      `#{@command}`
      @line_number = 0
      lines = []
      while line = File.open(@tmp_file).slice!(/.*?\n/m)
        lines << line.chomp
      end

      lines.each do |line|
        next if line == ""
        case @line_number
        when 0..1
        when 2
          line.delete!("\"")
          @first_keys = CSV.parse_line(line)
          pre_key = ""
          @first_keys.each_with_index do |key, index|
            if key.nil? || key == ""
              @first_keys[index] = pre_key
            else
              @first_keys[index] = @first_keys[index].gsub(/\s/, '_')
            end
            pre_key = @first_keys[index]
          end
        when 3
          line.delete!("\"")
          @second_keys = line.split(',')
          @first_keys.each_with_index do |key, index|
            @data_array[index] = {}
            @data_array[index][:first] = key
            @data_array[index][:second] = @second_keys[index]
          end
        else
          values = line.split(',')
          data = Hash.new { |hash,key| hash[key] = Hash.new {} }
          values.each_with_index do |v, index|
            data[@first_keys[index]][@second_keys[index]] = v
          end
          record = {
            'hostname' => @hostname,
            'dstat' => data
          }
          emit_tag = @tag.dup
          filter_record(emit_tag, Engine.now, record)
          router.emit(emit_tag, Engine.now, record)
        end

        @line_number += 1
        @last_time = Time.now
      end

      touch_or_truncate(@tmp_file)
    end

  end
end
