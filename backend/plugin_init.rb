require 'rubyXL'
require 'rubyXL/convenience_methods/workbook'
require 'rubyXL/convenience_methods/worksheet'
require 'rubyXL/convenience_methods/cell'

# This file contains overrides for methods in the following files:
# backend/app/lib/reports/report_generator.rb
# backend/app/model/reports/report_manager.rb

class ReportGenerator
  attr_accessor :report
  attr_accessor :sub_report_data_stack
  attr_accessor :sub_report_code_stack

  def initialize(report)
    @report = report
    @sub_report_data_stack = []
    @sub_report_code_stack = []
  end

  def generate(file)
    case(report.format)
    when 'json'
      generate_json(file)
    when 'html'
      generate_html(file)
    when 'pdf'
      generate_pdf(file)
    when 'rtf'
      generate_rtf(file)
    when 'xlsx'
      generate_xlsx(file)
    else
      generate_csv(file)
    end
  end

  def generate_xlsx(file)
    results = report.get_content
    xlsx = RubyXL::Workbook.new
    worksheet = xlsx[0]

    # Creates info columns (total count, repository name)
    starting_row = -1
    report.info.each do |key, value|
      starting_row += 1
      worksheet.add_cell(starting_row, 0, key.to_s)
      worksheet.add_cell(starting_row, 1, value.to_s)
    end

    #advance the row count to leave a blank row between the info columns and the report data
    starting_row = report.info.length + 1
    worksheet.add_cell(starting_row, 0, "")

    # Creates column headers
    header_row = results[0].keys
    header_row.each_with_index do |column_name, ind|
      worksheet.add_cell(starting_row.to_i, ind.to_i, column_name.to_s)
    end

    # Writes to worksheet
    results.each do |result|
      starting_row += 1
      # Set at -1 so that the column start count will always be 0
      starting_column = -1
      result.each do |key, value|
        starting_column +=1
        cell_value = worksheet.add_cell(starting_row, starting_column, value)
        # set correct formatting for date values: add more?
        if ['create_time', 'user_mtime', 'end', 'begin', 'accession_date', 'resource_create_time'].include? key.to_s
          cell_value.set_number_format('yyyy-mm-dd')
          cell_value.change_contents(value)
        end  
      end
    end
    file.write(xlsx.stream.read)
  end
end


module ReportManager

  @@registered_reports ||= {}

  ALLOWED_REPORT_FORMATS = ["xlsx", "json", "csv", "html", "pdf", "rtf"]

  def self.allowed_report_formats
    ALLOWED_REPORT_FORMATS
  end

  def self.register_report(report_class, opts)
    opts[:code] = report_class.code
    opts[:model] = report_class
    opts[:params] ||= []

    Log.warn("Report with code '#{opts[:code]}' already registered") if @@registered_reports.has_key?(opts[:code])

    @@registered_reports[opts[:code]] = opts

  end

  def self.registered_reports
    #Hacky thing to remove some or all of the native reports from the interface via the plugin
    @@registered_reports.delete_if { |key, _| ['accession_receipt_report', 'accession_deaccessions_list_report', 'accession_inventory_report', 'accession_subjects_names_classifications_list_report', 'accession_unprocessed_report', 'assessment_list_report', 'assessment_rating_report', 'created_accessions_report', 'agent_list_report', 'accession_rights_transferred_report', 'accession_report', 'digital_object_file_versions_report', 'digital_object_list_table_report', 'location_holdings_report', 'location_report', 'resource_deaccessions_list_report', 'resource_instances_list_report', 'resource_locations_list_report', 'resource_restrictions_list_report', 'resources_list_report', 'shelflist_report', 'subject_list_report', 'user_groups_report'].include? key }
    @@registered_reports
  end
end
