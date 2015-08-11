#!/usr/bin/env python
'''****************************************************************************
FILE: mapreduce_logfile.py

PURPOSE: Uses the map / reduce architecture to parse information from a logfile
    and then produce data and reports.

PROJECT: Land Satellites Data System Science Research and Development (LSRD)
    at the USGS EROS

LICENSE TYPE: NASA Open Source Agreement Version 1.3

AUTHOR: ngenetzky@usgs.gov

Notes:
    Expected Apache log format (single line replace (\n with space):
    '$remote_addr - $remote_user [$time_local] "$request" $status $bytes_sent
    "$http_referer" "$http_user_agent"

    The script assumes the url portion of $request does not have spaces, and
    will report BAD_PARSE if one is found. There are numerous other causes
    for a field to be labeled BAD_PARSE.
****************************************************************************'''
import datetime
import collections
import inspect
import urllib
'''
        Debug Functions
'''
log_badparse_in_reports = False
lines_that_failed_to_parse = []


def log_parses_that_failed(outfile):
    '''Will write lines that have failed to parse to a logfile

    Precondition:
        Functions have been called that attempted to parse lines in logfile.
    Postcondition:
        Logfile will contain entries that are separated by '\n'.
        Each entry will describe what couldn't be parsed and the line that was
             unable to be parsed.
    '''
    if(log_badparse_in_reports):
        global lines_that_failed_to_parse
        with open(outfile, 'w+') as fp:
            failed_parses = set(lines_that_failed_to_parse)
            fp.write('# lines that failed to parse: {0}'
                     .format(len(failed_parses)))
            for line in failed_parses:
                fp.write(str(line))


def fail_to_parse(value, line):
    '''Will add entry into list of lines that couldn't be parsed.

    Precondition:
        value contains a descriptive name of what value was attempting to be
             extracted.
        line contains the line that was attempted to be parsed.
    Postcondition
        returns 'BAD_PARSE'
    '''
    global lines_that_failed_to_parse
    lines_that_failed_to_parse.append('Failed to parse for {0} in <\n{1}>'
                                      .format(value, line))
    return 'BAD_PARSE'


def get_log_name():
    return 'bad_parse_log/{0}.txt'.format(inspect.stack()[1][3])


'''
        Helper Functions
'''


def substring_between(s, start, finish):
    '''Find string between two substrings'''
    end_of_start = s.index(start) + len(start)
    start_of_finish = s.index(finish, end_of_start)
    return s[end_of_start:start_of_finish]

'''
        Extract Data from line in logfile
'''


def get_rtcode(line):
    '''Obtain a return_code from a line of text

    Precondition: line is a ' ' separated list of data.
                Return code is the first int in the items from 6 to 11.
    Postcondition: return return_code
    '''
    data = line.split()
    if(data[8].isdigit()):
        return data[8]
    else:
        return fail_to_parse('rtcode', line)


def get_bytes(line):
    '''Obtain a return_code from a line of text

    Precondition: line is a ' ' separated list of data.
                Bytes downloaded is the second int in the items from 6 to 12.
    Postcondition: return bytes_downloaded
    '''
    data = line.split()
    if(data[9].isdigit()):
        return data[9]
    else:
        return fail_to_parse('bytes', line)


def get_datetime(line):
    time_local = substring_between(line, '[', '] "')
    try:
        return datetime.datetime.strptime(time_local,
                                          '%d/%b/%Y:%H:%M:%S -0500')
    except ValueError:
        return fail_to_parse('datetime', line)


def get_date(line):
    try:
        return get_datetime(line).date()
    except ValueError:
        return fail_to_parse('date', line)


def get_user_email(line):
    request = substring_between(line, '] "', '" ')
    request = urllib.unquote(request)
    try:
        return substring_between(request, 'orders/', '-')
    except ValueError:
        return fail_to_parse('user_email', line)


def get_scene_id(line):
    try:
        response_after_orderid = substring_between(line, 'orders/', '" ')
        return substring_between(response_after_orderid, '/', '.tar.gz')
    except ValueError:
        try:
            response_after_orderid = substring_between(line, 'orders/', '" ')
            return substring_between(response_after_orderid, '/', '.cksum')
        except ValueError:
            return fail_to_parse('sceneid', line)


def get_order_id(line):
    request = substring_between(line, '] "', '" ')
    try:
        return substring_between(request, 'orders/', '/')
    except ValueError:
        return fail_to_parse('orderid', line)

'''
        Filter helper functions
'''


def n_filters(filters, iterable):
        return (t for t in iterable if all(f(t) for f in filters))


def is_successful_request(line):
    return (get_rtcode(line) in ['200', '206'])


def is_404_request(line):
    return (get_rtcode(line) in ['404'])


def is_aborted_request(line):
    return (get_rtcode(line) in ['499', '304'])


def is_production_order(line):
    return ('"GET /orders/' in line)


def is_dswe_order(line):
    return (('"GET /provisional/dswe/' in line) or
            ('"GET /downloads/provisional/dswe/' in line))


def is_burned_area_order(line):
    return (('"GET /provisional/burned_area/' in line) or
            ('"GET /downloads/provisional/burned_area/' in line))


def is_ordered_by_usgs_gov_email(line):
    return ('@usgs.gov' in get_user_email(line))


def is_ordered_through_ee_interface(line):
    '''Use orderid to dtermine if ee interface was used to order

    Precondition: Assumes earth explorer's orderid contains two dashes
    Postcondition: Returns true if the ee explorer was used for the order.
    '''
    return (2 == get_order_id(line).count('-'))


def is_within_daterange(line, start_date, end_date):
    line_date = get_datetime(line)
    return ((start_date < line_date) and (line_date < end_date))


def is_in_this_month(line, month=datetime.datetime.now().month):
    return (get_datetime(line).month == month)


'''
        Mappers
'''


def map_line_to_custom_tuple(line):
    '''Extracts values from a line of text into tuple

    Precondition: line is a ' ' separated list of data.
        Preconditions for the following functions must also
            be satisfied: get_user_email, get_bytes, get_order_id, get_scene_id
    Postcondition: return tuple where len(tuple)==6
    '''
    remote_addr = line.split(' - ', 1)[0]
    dt = get_datetime(line).isoformat()
    user_email = get_user_email(line)
    bytes_sent = get_bytes(line)
    orderid = get_order_id(line)
    sceneid = get_scene_id(line)
    return (dt, remote_addr, user_email, orderid, sceneid, bytes_sent)


def map_line_to_rtcode_bytes(line):
    ''' Obtain (return_code, bytes_downloaded) from a line of text

    Precondition: line is a ' ' separated list of data.
                Return code is the first int in the items from 6 to 11.
                Bytes downloaded is the second int in the items from 6 to 12.
    Postcondition: return tuple where len(tuple)==2
                    list[0] is return_code
                    list[1] is bytes_downloaded
    '''
    # print('MAP<{0}>'.format(line))
    return (get_rtcode(line), int(get_bytes(line)))


def map_line_to_email_date(line):
    ''' Obtain (user_email, datetime_isoformat) from a line of text

    Precondition: line is a ' ' separated list of data.
        Between the first '[', and '] "' there exists the date similar
            to "07/Jun/2015"
        data[6] contains the user's request
        The user email is contained between "orders/" and the next "-"
    Postcondition: tuple is returned in the form (user_email, datetime)
        datetime will be in isoformat, similar to "2015-07-27"
    '''
    return (get_user_email(line), get_date(line).isoformat())


'''
        Reducers
'''


def reduce_count_total_and_perday(ordered_accum, next_tuple):
    '''Accumulate count per day and overall total.

    Precondition:
        next_tuple  has attribute '__getitem__'
        next_tuple[0] is A, next_tuple[1] is B
        ordered_accum is dictionary
    Postcondition:
        ordered_accum[A] is an OrderedDict.
        ordered_accum[A]['Total'] is incremented
        ordered_accum[A][B] is also incremented
    '''
    try:
        ordered_accum[next_tuple[0]]['Total'] += 1
    except KeyError:
        ordered_accum[next_tuple[0]] = collections.OrderedDict()
        ordered_accum[next_tuple[0]]['Total'] = 1

    try:
        ordered_accum[next_tuple[0]][next_tuple[1]] += 1
    except KeyError:
        ordered_accum[next_tuple[0]][next_tuple[1]] = 1
    return ordered_accum


def reduce_flatten_to_csv(accum, next_tuple):
    '''Combines tuple into string of values separated by commas'''
    if accum is None:
        accum = []
    accum.append(','.join(next_tuple))
    return accum


def reduce_append_value_to_key_list(accum_list, next_tuple):
    '''Appends value to existing value of accum_list[key]

    Used by reduce to create list of values within dict with identical keys
    Precondition:
        next_tuple  has attribute '__getitem__'
        next_tuple[0] is key, next_tuple[1] is value
        accum_list is dictionary
    Postcondition:
        returns a version of accum_list which either contains
            a new key/value or with a single key having a modified value.
    '''
    if next_tuple is None:
        return accum_list
    try:
        accum_list[next_tuple[0]].append(next_tuple[1])
    except KeyError:
        accum_list[next_tuple[0]] = []
        accum_list[next_tuple[0]].append(next_tuple[1])
    return accum_list


def reduce_accum_value_per_key(accum, next_tuple):
    '''Adds value to existing value of accum[key]

    Used by reduce to accumulate values within dict with identical keys
    Precondition:
        next_tuple  has attribute '__getitem__'
        next_tuple[0] is key, next_tuple[1] is value
        accum_bytes_per_code is dictionary
    Postcondition:
        returns a version of accum which either contains
            a new key/value or with a single key having a modified value.
    '''
    if next_tuple is None:
        return accum
    try:
        accum[next_tuple[0]] += next_tuple[1]
    except KeyError:
        accum[next_tuple[0]] = next_tuple[1]
    return accum


def reduce_count_occurrences_per_key(count, next_tuple):
    '''Adds 1 to existing value of count[key]

    Used by reduce to count occurrences of key
    Precondition:
        next_tuple  has attribute '__getitem__'
        next_tuple[0] is key, next_tuple[1] is value
        count is dictionary
    Postcondition:
        returns a version of count which either contains
            a new key/value or with a single key having a modified value.
    '''
    if next_tuple is None:
        return count
    try:
        count[next_tuple[0]] += 1
    except KeyError:
        count[next_tuple[0]] = 1
    return count


def reduce_count_per_keyvalue_occurrences(count, next_tuple):
    '''Adds 1 to existing value of count[(key,value)]

    Used by reduce to count occurrences of key
    Precondition:
        next_tuple  has attribute '__getitem__'
        next_tuple[0] is key, next_tuple[1] is value
        count is dictionary
    Postcondition:
        returns a version of count which either contains
            a new key/value or with a single key having a modified value.
    '''
    if next_tuple is None:
        return count
    try:
        count[(next_tuple[0], next_tuple[1])] += 1
    except KeyError:
        count[(next_tuple[0], next_tuple[1])] = 1
    return count

'''
        Map Reduce
'''


def mapreduce_csv(iterable):
    '''Extract data and returns list of strings with values sep. by commas

    Precondition:
    Postcondition:
        returns a list
        Each entry in list is a string of values with comma as delimiter
    '''
    tuples = map(map_line_to_custom_tuple, iterable)
    return reduce(reduce_flatten_to_csv, tuples, [])


def mapreduce_total_bytes(iterable):
    '''Extracts and accumulates bytes_downlaoded and then returns total

    Precondition:
    Postcondition:
        returns a list
        Each entry in list is a string of values with comma as delimiter
    '''
    list_of_bytes = map(get_bytes, iterable)
    return reduce(lambda total, x: int(total)+int(x), list_of_bytes, 0)


def mapreduce_total_occurrences(iterable):
    '''Basically len(iterable)'''
    list_of_bytes = map(lambda x: 1, iterable)
    return reduce(lambda total, x: int(total)+1, list_of_bytes, 0)


def mapreduce_bytes_per_code(iterable):
    '''Extracts return_code and downloaded_bytes then accumulates bytes per code

    Description:
        generate iterable of lists containing [return_code, downloaded_bytes]
        then all downloaded_bytes with the same return_code are accumulated and
        the answer is returned
    Precondition:
        iterable contains strings
    Postcondition:
        return bytes_per_code, a dictionary
        bytes_per_code contains return_code (key) and downloaded_bytes (value)
    '''
    bytes_per_code = {}
    code_bytes = map(map_line_to_rtcode_bytes, iterable)
    return reduce(reduce_accum_value_per_key, code_bytes, bytes_per_code)


def mapreduce_occurrences_per_code(iterable):
    '''Extracts return_code and downloaded_bytes then accumulates bytes per code

    Description:
        generate iterable of lists containing [return_code, downloaded_bytes]
        then the occurrences of return_code are counted and returned
    Precondition: iterable contains lines from an Apache formated log file
    Postcondition:
        return occurrences_per_code, a dictionary
        occurrences_per_code contains return_code (key) and occurrences (value)
    '''
    occurrences_per_code = {}
    code_bytes = map(map_line_to_rtcode_bytes, iterable)
    return reduce(reduce_count_occurrences_per_key,
                  code_bytes, occurrences_per_code)


def mapreduce_occurrences_per_email(iterable):
    occurrences_per_email = {}
    email_date = map(map_line_to_email_date, iterable)
    return reduce(reduce_count_occurrences_per_key,
                  email_date, occurrences_per_email)


def mapreduce_list_dates_per_email(iterable):
    list_per_email = {}
    email_date = map(map_line_to_email_date, iterable)
    return reduce(reduce_append_value_to_key_list,
                  email_date, list_per_email)


def mapreduce_occurrences_per_email_date(iterable):
    '''Extracts email and date into combined key, then counts occurrences in value

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Returns a dictionary
        dictionary will contain keys that contain email and date.
        dictionary values are the of the number of occurrences for that key
    '''
    occurrences_perday_and_peremail = {}
    email_date = map(map_line_to_email_date, iterable)
    return reduce(reduce_count_total_and_perday,
                  email_date, occurrences_perday_and_peremail)

'''
        Reports - Combine filters with mapreduce
'''


def report_bytes(iterable, filters):
    '''Reports the number of bytes downloaded

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report string will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    filtered = n_filters(filters, iterable)
    log_parses_that_failed(get_log_name())
    return str(mapreduce_total_bytes(filtered))


def report_requests(iterable, filters):
    '''Reports the number of requests

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report string will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    filtered = n_filters(filters, iterable)
    log_parses_that_failed(get_log_name())
    return str(mapreduce_total_occurrences(filtered))


def report_csv(iterable, filters):
    '''Report Entries: datetime,remote_addr,user_email,orderid,sceneid,bytes

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Returns line reports separated by '\n'
        Each line report contains:
            datetime,remote_addr,user_email,orderid,sceneid,bytes
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    filtered = n_filters(filters, iterable)
    log_parses_that_failed(get_log_name())
    return '\n'.join(mapreduce_csv(filtered))


def report_404_per_user_email(iterable, filters):
    ''' Will compile a report that provides total offenses per user

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: returns string of each user_report separated by '\n'
        Each user_report contains total_offenses and user_email
        Report is sorted from most offenses to least offenses
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    filtered = n_filters(filters, iterable)
    offenses_per_email = mapreduce_occurrences_per_email(filtered)

    sorted_num_of_offenses = sorted(offenses_per_email.iteritems(),
                                    key=lambda (k, v): v,
                                    reverse=True)
    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of offenses
        final_report.append('{1} {0}'.format(item[0], item[1]))

    log_parses_that_failed(get_log_name())

    return '\n'.join(final_report)


def report_404_perdate_peremail(iterable, filters):
    '''Will compile a report that provides total offenses per user
    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Returns user reports separated by '\n'
        User reports are sorted from most offenses to least offenses.
        Each user report contain total_bytes_downlaoded and email on first line
        Each user report will also contain a '\n' separated list daily reports.
        Each daily report includes date and number of occurrences on that date.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    filtered = n_filters(filters, iterable)

    offenses_perday_peremail = mapreduce_occurrences_per_email_date(filtered)

    sorted_num_of_offenses = sorted(offenses_perday_peremail.iteritems(),
                                    key=lambda t: t[1]['Total'], reverse=True)

    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of offense
        user_report = []
        total = item[1].popitem(last=False)[1]
        user_report.append(' '.join([str(total), item[0]]))
        for date, count in item[1].iteritems():
            user_report.append("\t {0} {1}"
                               .format(date, count))

        final_report.append('\n'.join(user_report))

    log_parses_that_failed(get_log_name())

    return '\n'.join(final_report)


'''
        Explicit Reports - Combine specific filters with mapreduce
'''


def report_succuessful_production_bytes(iterable):
    '''Reports the number of bytes downloaded for production products

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_production = filter(is_production_order, iterable)
    only_successful_production = filter(is_successful_request,
                                        only_production)
    log_parses_that_failed(get_log_name())

    return str(mapreduce_total_bytes(only_successful_production))


def report_succuessful_dswe_bytes(iterable):
    '''Reports the number of bytes downloaded for dswe products

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_dswe = filter(is_dswe_order, iterable)
    only_successful_dswe = filter(is_successful_request,
                                  only_dswe)
    log_parses_that_failed(get_log_name())

    return str(mapreduce_total_bytes(only_successful_dswe))


def report_succuessful_burned_area_bytes(iterable):
    '''Reports the number of bytes downloaded for burned_area products

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_burned_area = filter(is_burned_area_order, iterable)
    only_successful_burned_area = filter(is_successful_request,
                                         only_burned_area)
    log_parses_that_failed(get_log_name())

    return str(mapreduce_total_bytes(only_successful_burned_area))


def report_succuessful_production_requests(iterable):
    '''Reports the number of requests for burned_area products

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_production = filter(is_production_order, iterable)
    only_successful_production = filter(is_successful_request,
                                        only_production)
    log_parses_that_failed(get_log_name())

    return str(mapreduce_total_occurrences(only_successful_production))


def report_succuessful_dswe_requests(iterable):
    '''Reports the number of requests for burned_area products

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_dswe = filter(is_dswe_order, iterable)
    only_successful_dswe = filter(is_successful_request,
                                  only_dswe)
    log_parses_that_failed(get_log_name())

    return str(mapreduce_total_occurrences(only_successful_dswe))


def report_succuessful_burned_area_requests(iterable):
    '''Reports the number of requests for burned_area products

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Report will contain a single integer.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_burned_area = filter(is_burned_area_order, iterable)
    only_successful_burned_area = filter(is_successful_request,
                                         only_burned_area)
    log_parses_that_failed(get_log_name())

    return str(mapreduce_total_occurrences(only_successful_burned_area))


def report_404_per_user_email_on_production_orders(iterable):
    ''' Will compile a report that provides total offenses per user

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: returns string of each user_report separated by '\n'
        Each user_report contains total_offenses and user_email
        Report is sorted from most offenses to least offenses
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_production = filter(is_production_order, iterable)
    production_404requests = filter(is_404_request, only_production)
    offenses_per_email = mapreduce_occurrences_per_email(production_404requests)

    sorted_num_of_offenses = sorted(offenses_per_email.iteritems(),
                                    key=lambda (k, v): v,
                                    reverse=True)
    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of offenses
        final_report.append('{1} {0}'.format(item[0], item[1]))

    log_parses_that_failed(get_log_name())

    return '\n'.join(final_report)


def report_404_perdate_peremail_on_production_orders(iterable):
    '''Will compile a report that provides total offenses per user
    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Returns user reports separated by '\n'
        User reports are sorted from most offenses to least offenses.
        Each user report contain total_bytes_downlaoded and email on first line
        Each user report will also contain a '\n' separated list daily reports.
        Each daily report includes date and number of occurrences on that date.
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_production = filter(is_production_order, iterable)
    filtered = filter(is_404_request, only_production)

    offenses_perday_peremail = mapreduce_occurrences_per_email_date(filtered)

    sorted_num_of_offenses = sorted(offenses_perday_peremail.iteritems(),
                                    key=lambda t: t[1]['Total'], reverse=True)

    final_report = []
    for item in sorted_num_of_offenses:
        # item[0] = email, item[1] = number of offense
        user_report = []
        total = item[1].popitem(last=False)[1]
        user_report.append(' '.join([str(total), item[0]]))
        for date, count in item[1].iteritems():
            user_report.append("\t {0} {1}"
                               .format(date, count))

        final_report.append('\n'.join(user_report))
    log_parses_that_failed(get_log_name())

    return '\n'.join(final_report)


def report_csv_of_successful_orders(iterable):
    '''Report Entries: datetime,remote_addr,user_email,orderid,sceneid,bytes

    Precondition: iterable contains lines from an Apache formated log file
    Postcondition: Returns line reports separated by '\n'
        Each line report contains:
            datetime,remote_addr,user_email,orderid,sceneid,bytes
    Note: If a value was unable to be parsed then the value will be reported
            as 'BAD_PARSE'.
    '''
    only_production = filter(is_production_order, iterable)
    only_successful_production = filter(is_successful_request,
                                        only_production)
    log_parses_that_failed(get_log_name())
    return '\n'.join(mapreduce_csv(only_successful_production))
'''
        Access a report by running script
'''


if __name__ == '__main__':
    print('This file should only be used as an import')


