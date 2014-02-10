import lta, utils

####################################################################################################################
# Validates all the projection params if the user chose to reproject
####################################################################################################################
def _projection(parms, options, option_errors):

    #we only need to check this if the user chose to reproject
    if parms.has_key('reproject') and parms['reproject'] == 'on':

        #set the reproject status True in the product options
        options['reproject'] = True
        
        if parms.has_key('target_projection'):
            
            if parms['target_projection'] == 'aea':
                options['target_projection'] = parms['target_projection']
                
                if parms.has_key('central_meridian') and utils.is_number(parms['central_meridian']):
                    options['central_meridian'] = float(parms['central_meridian'])
                else:
                    option_errors.append(
                    "Please provide a valid central meridian value")
                
                if parms.has_key('false_easting') and utils.is_number(parms['false_easting']):
                    options['false_easting'] = float(parms['false_easting'])
                else:
                    option_errors.append("Please provide a valid false_easting value")
                
                if parms.has_key('false_northing')and utils.is_number(parms['false_northing']):
                    options['false_northing'] = float(parms['false_northing'])
                else:
                    option_errors.append("Please provide a valid false_northing value")
                
                if parms.has_key('std_parallel_1') and utils.is_number(parms['std_parallel_1']):
                    options['std_parallel_1'] = float(parms['std_parallel_1'])
                else:
                    option_errors.append("Please provide a valid value for the 1st standard parallel")
                
                if parms.has_key('std_parallel_2') and utils.is_number(parms['std_parallel_2']):
                    options['std_parallel_2'] = float(parms['std_parallel_2'])
                else:
                    option_errors.append("Please provide a valid value for the 2nd standard parallel")
                    
                if parms.has_key('origin_lat') and utils.is_number(parms['origin_lat']):
                    options['origin_lat'] = float(parms['origin_lat'])
                else:
                    option_errors.append("Please provide a valid latitude of origin")
                    
                if parms.has_key('datum') and parms['datum'] in ('nad27', 'nad83', 'wgs84'):
                    options['datum'] = parms['datum']
                else:
                    if len(parms['datum']) < 1:
                        option_errors.append("No datum was provided for reprojection")
                    else:
                        option_errors.append("%s is an unsupported datum" % parms['datum'])
            
            elif parms['target_projection'] == 'sinu':
                
                options['target_projection'] = parms['target_projection']
                
                if parms.has_key('central_meridian') and utils.is_number(parms['central_meridian']):
                    options['central_meridian'] = float(parms['central_meridian'])
                else:
                    option_errors.append("Please provide a valid central meridian value")
                
                if parms.has_key('false_easting') and utils.is_number(parms['false_easting']):
                    options['false_easting'] = float(parms['false_easting'])
                else:
                    option_errors.append("Please provide a valid false_easting value")
                
                if parms.has_key('false_northing')and utils.is_number(parms['false_northing']):
                    options['false_northing'] = float(parms['false_northing'])
                else:
                    option_errors.append("Please provide a valid false_northing value")
                
            elif parms['target_projection'] == 'utm':
                options['target_projection'] = parms['target_projection']
                
                if parms.has_key('utm_zone') \
                and str(parms['utm_zone']).isdigit() \
                and int(parms['utm_zone']) in range(1, 61):
                    options['utm_zone'] = int(parms['utm_zone'])
                else:
                    option_errors.append("Please provide a utm zone between 1 and 60")
                    
                if parms.has_key('utm_north_south') and parms['utm_north_south'] in ('north', 'south'):
                    if parms['utm_north_south'] == 'north':
                        options['utm_north_south'] = 'north'
                    else:
                        options['utm_north_south'] = 'south'
                else:
                    option_errors.append("Please select north or south for the UTM zone")
                
            elif parms['target_projection'] == 'lonlat':
                options['target_projection'] = parms['target_projection']
            else:
                if len(parms['target_projection']) > 1:
                    option_errors.append("%s is not a supported projection" % parms['target_projection'])
                else:
                    option_errors.append("No target projection provided")
                
            
        
        #make sure a projection was supplied and it was one of aea, lonlat, sinu, utm
            
####################################################################################################################
# Validates all the image extents for image resizing/reshaping
####################################################################################################################
def _boundingbox(parms, options, option_errors):
    if parms.has_key('image_extents') and parms['image_extents'] == 'on':
        #set the image_extents status True in product options
        options['image_extents'] = True
        
        #make sure we got upper left x,y and lower right x,y vals
        if parms.has_key('minx') and utils.is_number(parms['minx']):
            options['minx'] = float(parms['minx'])
        else:
            option_errors.append("Please provide a valid upper left x value")
            
        if parms.has_key('maxx') and utils.is_number(parms['maxx']):
            options['maxx'] = float(parms['maxx'])
        else:
            option_errors.append("Please provide a valid lower right x value")
            
        if parms.has_key('miny') and utils.is_number(parms['miny']):
            options['miny'] = float(parms['miny'])
        else:
            option_errors.append("Please provide a valid lower right y value")
            
        if parms.has_key('maxy') and utils.is_number(parms['maxy']):
            options['maxy'] = float(parms['maxy'])
        else:
            option_errors.append("Please provide a valid upper left y value")
            
        #make sure values make some sort of sense
        if options['minx'] >= options['maxx']:
            option_errors.append("Upper left x value must be less than lower right x value")
            
        if options['miny'] >= options['maxy']:
            option_errors.append("Lower right y value must be less than upper left y value")
            
        #at some point we will need to restrict the inputted size, otherwise someone could do something
        #nasty like parmsing an image be framed from -180, -90 to 180, 90.  This would be ridiculous and would
        #blow up the disk space on the processing nodes.
        
        
####################################################################################################################
# Ensures the supplied pixel_size and pixel_size_units are valid
####################################################################################################################
def _pixelsize(parms, options, option_errors):
    if parms.has_key('resize') and parms['resize'] == 'on':
        #set the resize status True in the product options
        options['resize'] = True
        
        # 
        #Handle pixel_size_unit validation
        #
        if not parms.has_key('pixel_size_units') or parms['pixel_size_units'] == None:
            option_errors.append("Target pixel size units not recognized")
        else:
            units = parms['pixel_size_units'].strip()
            if units == 'dd' or units == 'meters':
                options['pixel_size_units'] = units
            else:
                option_errors.append("Unknown pixel size units provided:%s" % units)
            
        #
        #Now validate the supplied pixel_size.  Must be between 30 and 1000 meters or .0002695 to .0089831 dd
        #
        if not parms.has_key('pixel_size') or parms['pixel_size'] == None:
            option_errors.append("Please enter desired pixel size")
        else:
            pixel_size = parms['pixel_size']
            if not utils.is_number(pixel_size):
                option_errors.append("Please enter a pixel size between 30 and 1000 meters or .0002695 to .0089831 dd")
            else:
                if options['pixel_size_units'] != None:
                    pixel_size = float(pixel_size)
                    if options['pixel_size_units'] ==  'meters':
                        if pixel_size >= 30 and pixel_size <= 1000:
                            options['pixel_size'] = pixel_size
                        else:
                            option_errors.append("Please enter a pixel size between 30 and 1000 meters")
                    else:
                        if pixel_size >= .0002695 and pixel_size <= .0089831:
                            options['pixel_size'] = pixel_size
                        else:
                            option_errors.append("Please enter a pixel size between .0002695 and .0089831 decimal degrees")
                        
                 
####################################################################################################################
#Makes sure that the product options selected make sense and are in bounds
####################################################################################################################
def product(parms):
    prod_option_errors = list()
    default_options = utils.get_default_options()
            
    #Collect parmsed products.
    for o in default_options.iterkeys():
        #if parms.has_key(o) and parms[o] == True:
        if parms.has_key(o) and (parms[o] == True or str(parms[o]).lower() == 'on'):
            default_options[o] = True
            
           
    _projection(parms, default_options, prod_option_errors)
    _boundingbox(parms, default_options, prod_option_errors)
    _pixelsize(parms, default_options, prod_option_errors)
        
    return (default_options,prod_option_errors)
        
    
####################################################################################################################
#Ensures an email was supplied
####################################################################################################################
def _email(parms, context, errors):
    
    #start off by making sure we have an email address.
    if not parms.has_key('email') or not utils.email(parms['email']):    
        errors['email'] = "Please provide a valid email address"
    else:
        context['email'] = parms['email']
            
####################################################################################################################
#Checks for the prescence of a scenelist, checks it for errors.
####################################################################################################################
def _files_and_scenes(parms, context, errors, scene_errors):
    #make sure we have an uploaded scenelist file
    if not parms.FILES.has_key("scenelist"):
        errors['file'] = "Please provide a scene list and include at least one scene for processing."
    else:
        #there was a file attached to the parms.  make sure its not empty.
        orderfile = parms.FILES['scenelist']
        lines = orderfile.read().split('\n')

        if len(lines) <= 0:
            errors['file'] = "No scenes found in your scenelist. Please include at least one scene for processing."
        else:
            #Simple length and prefix checks for scenelist items   
            context['scenelist'] = set()
            for line in lines:
                line = line.strip()
                if line.find('.tar.gz') != -1:
                    line = line[0:line.index('.tar.gz')]
                
                if len(line) >= 15 and (line.startswith("LT") or line.startswith("LE")):
                    context['scenelist'].add(line)

            #Run the submitted list by LTA so they can make sure the items are in the inventory
            verified_scenes = lta.LtaServices().verify_scenes(list(context['scenelist']))
            for sc,valid in verified_scenes.iteritems():
                if valid == 'false':
                    scene_errors.append("%s not found in Landsat inventory" % sc)
         
            #after all that validation, make sure there's actually something left to order
            if len(context['scenelist']) < 1:
                scene_errors.append("No scenes found in scenelist. Please provide at least one scene for processing")

####################################################################################################################
# Validates that a user selected at least one option
####################################################################################################################
def _product_was_selected(parms, errors):
    '''Verifies that at least one product was selected for processing'''
    ok = False
    for key in utils.get_default_product_options().iterkeys():
        if parms.has_key(key):
            ok = True
            break
    if not ok:
        errors.append("Please select at least one product for processing")
    

        
####################################################################################################################
# Validation for the order parms from users
# This is the single method that needs to be called
####################################################################################################################
def userinput(parms):
        
    context, errors, scene_errors = {},{}, list()
        
    _email(parms, context, errors)
    _files_and_scenes(parms, context, errors, scene_errors)
    _product_was_selected(parms, errors)
    #product_options(parms, context, errors)        

    #Look for an order_description and put it into the context if available
    if parms.has_key('order_description'):
        context['order_description'] = parms['order_description']
        
    return (context, errors, scene_errors)
