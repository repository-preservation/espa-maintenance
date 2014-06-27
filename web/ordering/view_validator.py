import lta
import core
from models import Order

#TODO -- Build a class hierarchy that will clean this stuff up...
# AbstractValidator() class should take the request dictionary in and
# define the validate() method to be called for each subclass.
# should also define an errors list()/array 
def validate_projection_params(request, options, option_errors):
    '''Validates the projection parameters supplied if the user has
    requested reprojection

    Keyword args:
    request -- HTTP request object
    options -- Dictionary of options to be returned to the caller
    option_errors -- List of errors to be returned to the end user

    Return:
    No return. Keyword parameters are passed by reference.
    '''

    # build some aliases to clean the code up
    P = request.POST
    O = options
    E = option_errors
    ISNBR = core.is_number

    #we only need to check this if the user chose to reproject
    if 'reproject' in P and P['reproject'] == 'on':

        #set the reproject status True in the product options
        O['reproject'] = True

        if 'target_projection' in P:

            if P['target_projection'] == 'aea':
                O['target_projection'] = P['target_projection']

                if 'central_meridian' in P and ISNBR(P['central_meridian']):
                        O['central_meridian'] = float(P['central_meridian'])
                else:
                    E.append("Please provide a valid central meridian value")

                if 'false_easting' in P and ISNBR(P['false_easting']):
                        O['false_easting'] = float(P['false_easting'])
                else:
                    E.append("Please provide a valid false_easting value")

                if 'false_northing' in P and ISNBR(P['false_northing']):
                    O['false_northing'] = float(P['false_northing'])
                else:
                    E.append("Please provide a valid false_northing value")

                if 'std_parallel_1' in P and ISNBR(P['std_parallel_1']):
                    O['std_parallel_1'] = float(P['std_parallel_1'])
                else:
                    E.append("Please provide a valid \
                             value for the 1st standard parallel")

                if 'std_parallel_2' in P and ISNBR(P['std_parallel_2']):
                    O['std_parallel_2'] = float(P['std_parallel_2'])
                else:
                    E.append("Please provide a valid \
                              value for the 2nd standard parallel")

                if 'origin_lat' in P and ISNBR(P['origin_lat']):
                    O['origin_lat'] = float(P['origin_lat'])
                else:
                    E.append("Please provide a valid latitude of origin")

                if 'datum' in P and P['datum'] in ('nad27', 'nad83', 'wgs84'):
                    O['datum'] = P['datum']
                else:
                    if len(P['datum']) < 1:
                        E.append("No datum was provided for reprojection")
                    else:
                        E.append("%s is an unsupported datum" % P['datum'])

            elif P['target_projection'] == 'sinu':

                O['target_projection'] = P['target_projection']

                if 'central_meridian' in P and ISNBR(P['central_meridian']):
                    O['central_meridian'] = float(P['central_meridian'])
                else:
                    E.append("Please provide a valid central meridian value")

                if 'false_easting' in P and ISNBR(P['false_easting']):
                    O['false_easting'] = float(P['false_easting'])
                else:
                    E.append("Please provide a valid false_easting value")

                if 'false_northing' in P and ISNBR(P['false_northing']):
                    O['false_northing'] = float(P['false_northing'])
                else:
                    E.append("Please provide a valid false_northing value")

            elif P['target_projection'] == 'ps':

                O['target_projection'] = P['target_projection']

                if 'longitude_pole' in P and ISNBR(P['longitude_pole']):
                    O['longitude_pole'] = float(P['longitude_pole'])
                else:
                    E.append("Please provide a valid central meridian value")

                if 'latitude_true_scale' in P\
                    and ISNBR(P['latitude_true_scale']\
                    and P['latitude_true_scale'] in range(60, 91) or\
                    P['latitude_true_scale'] in range(-90, -59)):
                        
                    O['latitude_true_scale'] = float(P['latitude_true_scale'])
                else:
                    E.append("Please provide a valid Latitude True Scale\
                        value in the ranges of -60.0 to -90.0 or \
                        60.0 to 90.0")
                    
                if 'false_easting' in P and ISNBR(P['false_easting']):
                    O['false_easting'] = float(P['false_easting'])
                else:
                    E.append("Please provide a valid false_easting value")

                if 'false_northing' in P and ISNBR(P['false_northing']):
                    O['false_northing'] = float(P['false_northing'])
                else:
                    E.append("Please provide a valid false_northing value")
            elif P['target_projection'] == 'utm':
                O['target_projection'] = P['target_projection']

                if 'utm_zone' in P \
                        and str(P['utm_zone']).isdigit() \
                        and int(P['utm_zone']) in range(1, 61):
                    O['utm_zone'] = int(P['utm_zone'])
                else:
                    E.append("Please provide a utm zone between 1 and 60")

                if 'utm_north_south' in P \
                        and P['utm_north_south'] in ('north', 'south'):
                    O['utm_north_south'] = P['utm_north_south']
                else:
                    E.append("Please select north or south for the UTM zone")

            elif P['target_projection'] == 'lonlat':
                O['target_projection'] = P['target_projection']
            else:
                if len(P['target_projection']) > 1:
                    E.append("%s is not a supported projection"
                             % P['target_projection'])
                else:
                    E.append("No target projection provided")


def validate_boundingbox_params(request, options, option_errors):
    '''Validates the image extents are present and rational

    Keyword args:
    request -- HTTP request object
    options -- Dictionary of options to be accessed by the caller
    option_errors -- List of errors to be returned to caller

    Return:
    No return. Keyword parameters are passed by reference.
    '''
    # build some aliases to clean the code up
    P = request.POST
    O = options
    E = option_errors
    ISNBR = core.is_number

    if 'image_extents' in P and P['image_extents'] == 'on':
        # set the image_extents status True in product options
        O['image_extents'] = True

        # make sure we got upper left x,y and lower right x,y vals
        if 'minx' in P and ISNBR(P['minx']):
            O['minx'] = float(P['minx'])
        else:
            E.append("Please provide a valid upper left x value")

        if 'maxx' in P and ISNBR(P['maxx']):
            O['maxx'] = float(P['maxx'])
        else:
            E.append("Please provide a valid lower right x value")

        if 'miny' in P and ISNBR(P['miny']):
            O['miny'] = float(P['miny'])
        else:
            E.append("Please provide a valid lower right y value")

        if 'maxy' in P and ISNBR(P['maxy']):
            O['maxy'] = float(P['maxy'])
        else:
            E.append("Please provide a valid upper left y value")

        # make sure values make some sort of sense
        if O['minx'] >= O['maxx']:
            E.append("Upper left x value must be \
                     less than lower right x value")

        if O['miny'] >= O['maxy']:
            E.append("Lower right y value must be \
                      less than upper left y value")

        # at some point we will need to restrict the inputted size,
        # otherwise someone could do something nasty like requesting an
        # image be framed from -180, -90 to 180, 90.  This would be
        # ridiculous and would blow up the disk space on the processing nodes.


def validate_pixelsize_params(request, options, option_errors):
    '''Validates that the requested pixel size if valid if the user
    chose to resize/resample.

    Keyword args:
    request -- HTTP request object
    options -- Dictionary of options to be accessed by the caller
    option_errors -- List of errors to be used by the caller

    Return:
    No return. Keyword parameters are passed by reference.
    '''

    # build some aliases to clean the code up
    P = request.POST
    O = options
    E = option_errors
    ISNBR = core.is_number

    if 'resize' in P and P['resize'] == 'on':
        #set the resize status True in the product options
        O['resize'] = True

        #
        # Handle pixel_size_unit validation
        #
        if not 'pixel_size_units' in P or not P['pixel_size_units']:
            E.append("Target pixel size units not recognized")
        else:
            units = P['pixel_size_units'].strip()

            if units in ['dd', 'meters']:
                O['pixel_size_units'] = units
            else:
                E.append("Unknown pixel size units provided:%s" % units)

        #
        # Now validate the supplied pixel_size.
        # Must be between 30 and 1000 meters or .0002695 to .0089831 dd
        #
        if 'pixel_size' not in P or P['pixel_size'] is None:
            E.append("Please enter desired pixel size")
        else:
            pixel_size = P['pixel_size']

            if not ISNBR(pixel_size):
                E.append("Please enter a pixel size between \
                          30 and 1000 meters or .0002695 to .0089831 dd")
            else:
                if O['pixel_size_units'] is not None:

                    pixel_size = float(pixel_size)

                    if O['pixel_size_units'] == 'meters':

                        if pixel_size >= 30 and pixel_size <= 1000:
                            O['pixel_size'] = pixel_size
                        else:
                            E.append("Please enter a pixel size \
                                      between 30 and 1000 meters")
                    else:
                        if pixel_size >= .0002695 and pixel_size <= .0089831:
                            O['pixel_size'] = pixel_size
                        else:
                            E.append("Please enter a pixel size between \
                                     .0002695 and .0089831 decimal degrees")


def validate_product_options(request):
    '''Validates options (such as pixel size, bounding coordinates, etc) are
    valid for the request.

    Keyword args:
    request -- HTTP request object

    Return:
    Tuple of (default_options, errors) where
        default_options is dict() 
        errors is list()
    '''
    # build some aliases to clean the code up
    P = request.POST

    prod_errors = list()
    default_options = Order.get_default_options()

    #Collect requested products.
    for o in default_options.iterkeys():
        #if P.has_key(o) and P[o] == True:
        if o in P and (P[o] is True or str(P[o]).lower() == 'on'):
            default_options[o] = True

    validate_projection_params(request, default_options, prod_errors)
    validate_boundingbox_params(request, default_options, prod_errors)
    validate_pixelsize_params(request, default_options, prod_errors)

    return (default_options, prod_errors)


def validate_scenelist(request):
    '''Ensures that a scene list was provided and checks the list
    for errors, bad values

    Keyword args:
    request -- HTTP request object
    
    Return:
    A tuple of (scenelist, errors) where 
       scene_list = set() or None
       errors = list() or None
           
    '''
   
    errors = list()
    scene_list = set()
    
    # make sure we have an uploaded scenelist file
    if not 'scenelist' in request.FILES:
        errors.append("Please provide a scene list and \
                         include at least one scene for processing.")
    else:
        # there was a file attached to the request.  make sure its not empty.
        orderfile = request.FILES['scenelist']
        
        lines = orderfile.read().split('\n')

        if len(lines) <= 0:
            errors.append("Please include at least one scene for processing.")
        else:
            # Simple length and prefix checks for scenelist items
            for line in lines:
                line = line.strip()
                if line.find('.tar.gz') != -1:
                    line = line[0:line.index('.tar.gz')]

                if len(line) >= 15 \
                        and (line.startswith("LT") or line.startswith("LE")):
                    scene_list.add(line)

            # Run the submitted list by LTA so they can make sure
            # the items are in the inventory
            c = lta.OrderWrapperServiceClient()
            verified_scenes = c.verify_scenes(list(scene_list))
            for sc, valid in verified_scenes.iteritems():
                if valid == 'false':
                    errors.append("%s not found in Landsat inventory"
                                        % sc)

            # after all that validation, make sure there's
            # actually something left to order
            if len(scene_list) < 1:
                scene_list = None
                errors.append("No scenes found in order file. \
                Please provide at least one scene for processing")

    
    if len(errors) < 1:
        errors = None
        
    return (scene_list, errors)


def product_is_selected(request):
    '''Verifies that at least one product was selected for processing

    Keyword args:
    request -- HTTP request object

    Return:
    True if a product is selected, False if not
    '''
    
    for key in Order.get_default_product_options().iterkeys():
        if key in request.POST:
            return True
    
    return False



def validate_input_params(request):
    '''This is validation for the form request from users

    Keyword args:
    request -- HTTP request object

    Return:
    Tuple(dict1(), list()) where dict1 is the context,
        dict2 are request errors and list are scene errors
    '''

    scene_list, errors = validate_scenelist(request)
    
    if not product_is_selected(request):
        if not errors:
            errors = list()
        errors.append("Please select at least one output product.")

    return (scene_list, errors)
    
        
