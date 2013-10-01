import lta, core

####################################################################################################################
# Validates all the projection params if the user chose to reproject
####################################################################################################################
def validate_projection_params(request, options, option_errors):
    #we only need to check this if the user chose to reproject
    if request.POST.has_key('reproject') and request.POST['reproject'] == 'on':
        #set the reproject status True in the product options
        options['reproject'] = True
        
        if request.POST.has_key('target_projection'):
            
            if request.POST['target_projection'] == 'aea':
                options['target_projection'] = request.POST['target_projection']
                
                if request.POST.has_key('central_meridian') and core.is_number(request.POST['central_meridian']):
                    options['central_meridian'] = float(request.POST['central_meridian'])
                else:
                    option_errors.append("Please provide a valid central meridian value")
                
                if request.POST.has_key('false_easting') and core.is_number(request.POST['false_easting']):
                    options['false_easting'] = float(request.POST['false_easting'])
                else:
                    option_errors.append("Please provide a valid false_easting value")
                
                if request.POST.has_key('false_northing')and core.is_number(request.POST['false_northing']):
                    options['false_northing'] = float(request.POST['false_northing'])
                else:
                    option_errors.append("Please provide a valid false_northing value")
                
                if request.POST.has_key('std_parallel_1') and core.is_number(request.POST['std_parallel_1']):
                    options['std_parallel_1'] = float(request.POST['std_parallel_1'])
                else:
                    option_errors.append("Please provide a valid value for the 1st standard parallel")
                
                if request.POST.has_key('std_parallel_2') and core.is_number(request.POST['std_parallel_2']):
                    options['std_parallel_2'] = float(request.POST['std_parallel_2'])
                else:
                    option_errors.append("Please provide a valid value for the 2nd standard parallel")
                    
                if request.POST.has_key('origin_lat') and core.is_number(request.POST['origin_lat']):
                    options['origin_lat'] = float(request.POST['origin_lat'])
                else:
                    option_errors.append("Please provide a valid latitude of origin")
                    
                if request.POST.has_key('datum') and request.POST['datum'] in ('nad27', 'nad83', 'wgs84'):
                    options['datum'] = request.POST['datum']
                else:
                    if len(request.POST['datum']) < 1:
                        option_errors.append("No datum was provided for reprojection")
                    else:
                        option_errors.append("%s is an unsupported datum" % request.POST['datum'])
            
            elif request.POST['target_projection'] == 'sinu':
                
                options['target_projection'] = request.POST['target_projection']
                
                if request.POST.has_key('central_meridian') and core.is_number(request.POST['central_meridian']):
                    options['central_meridian'] = float(request.POST['central_meridian'])
                else:
                    option_errors.append("Please provide a valid central meridian value")
                
                if request.POST.has_key('false_easting') and core.is_number(request.POST['false_easting']):
                    options['false_easting'] = float(request.POST['false_easting'])
                else:
                    option_errors.append("Please provide a valid false_easting value")
                
                if request.POST.has_key('false_northing')and core.is_number(request.POST['false_northing']):
                    options['false_northing'] = float(request.POST['false_northing'])
                else:
                    option_errors.append("Please provide a valid false_northing value")
                
            elif request.POST['target_projection'] == 'utm':
                options['target_projection'] = request.POST['target_projection']
                
                if request.POST.has_key('utm_zone') \
                and str(request.POST['utm_zone']).isdigit() \
                and int(request.POST['utm_zone']) in range(1, 61):
                    options['utm_zone'] = int(request.POST['utm_zone'])
                else:
                    option_errors.append("Please provide a utm zone between 1 and 60")
                    
                if request.POST.has_key('utm_north_south') and request.POST['utm_north_south'] in ('north', 'south'):
                    if request.POST['utm_north_south'] == 'north':
                        options['utm_north_south'] = 'north'
                    else:
                        options['utm_north_south'] = 'south'
                else:
                    option_errors.append("Please select north or south for the UTM zone")
                
            elif request.POST['target_projection'] == 'lonlat':
                options['target_projection'] = request.POST['target_projection']
            else:
                if len(request.POST['target_projection']) > 1:
                    option_errors.append("%s is not a supported projection" % request.POST['target_projection'])
                else:
                    option_errors.append("No target projection provided")
                
            
        
        #make sure a projection was supplied and it was one of aea, lonlat, sinu, utm
            
####################################################################################################################
# Validates all the image extents for image resizing/reshaping
####################################################################################################################
def validate_boundingbox_params(request, options, option_errors):
    if request.POST.has_key('image_extents') and request.POST['image_extents'] == 'on':
        #set the image_extents status True in product options
        options['image_extents'] = True
        
        #make sure we got upper left x,y and lower right x,y vals
        if request.POST.has_key('minx') and core.is_number(request.POST['minx']):
            options['minx'] = float(request.POST['minx'])
        else:
            option_errors.append("Please provide a valid upper left x value")
            
        if request.POST.has_key('maxx') and core.is_number(request.POST['maxx']):
            options['maxx'] = float(request.POST['maxx'])
        else:
            option_errors.append("Please provide a valid lower right x value")
            
        if request.POST.has_key('miny') and core.is_number(request.POST['miny']):
            options['miny'] = float(request.POST['miny'])
        else:
            option_errors.append("Please provide a valid lower right y value")
            
        if request.POST.has_key('maxy') and core.is_number(request.POST['maxy']):
            options['maxy'] = float(request.POST['maxy'])
        else:
            option_errors.append("Please provide a valid upper left y value")
            
        #make sure values make some sort of sense
        if options['minx'] >= options['maxx']:
            option_errors.append("Upper left x value must be less than lower right x value")
            
        if options['miny'] >= options['maxy']:
            option_errors.append("Lower right y value must be less than upper left y value")
            
        #at some point we will need to restrict the inputted size, otherwise someone could do something
        #nasty like requesting an image be framed from -180, -90 to 180, 90.  This would be ridiculous and would
        #blow up the disk space on the processing nodes.
        
        
####################################################################################################################
# Ensures the supplied pixel_size and pixel_size_units are valid
####################################################################################################################
def validate_pixelsize_params(request, options, option_errors):
    if request.POST.has_key('resize') and request.POST['resize'] == 'on':
        #set the resize status True in the product options
        options['resize'] = True
        
        # 
        #Handle pixel_size_unit validation
        #
        if not request.POST.has_key('pixel_size_units') or request.POST['pixel_size_units'] == None:
            option_errors.append("Target pixel size units not recognized")
        else:
            units = request.POST['pixel_size_units'].strip()
            if units == 'dd' or units == 'meters':
                options['pixel_size_units'] = units
            else:
                option_errors.append("Unknown pixel size units provided:%s" % units)
            
        #
        #Now validate the supplied pixel_size.  Must be between 30 and 1000 meters or .0002695 to .0089831 dd
        #
        if not request.POST.has_key('pixel_size') or request.POST['pixel_size'] == None:
            option_errors.append("Please enter desired pixel size")
        else:
            pixel_size = request.POST['pixel_size']
            if not core.is_number(pixel_size):
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
def validate_product_options(request):
    prod_option_errors = list()
    default_options = core.get_default_options()
            
    #Collect requested products.
    for o in default_options.iterkeys():
        #if request.POST.has_key(o) and request.POST[o] == True:
        if request.POST.has_key(o) and (request.POST[o] == True or str(request.POST[o]).lower() == 'on'):
            default_options[o] = True
            
           
    validate_projection_params(request, default_options, prod_option_errors)
    validate_boundingbox_params(request, default_options, prod_option_errors)
    validate_pixelsize_params(request, default_options, prod_option_errors)
        
    return (default_options,prod_option_errors)
        
    
####################################################################################################################
#Ensures an email was supplied
####################################################################################################################
def validate_email(request, context, errors):
        
    #start off by making sure we have an email address.
    if not request.POST.has_key('email') or not core.validate_email(request.POST['email']):    
        errors['email'] = "Please provide a valid email address"
    else:
        context['email'] = request.POST['email']
            
####################################################################################################################
#Checks for the prescence of a scenelist, checks it for errors.
####################################################################################################################
def validate_files_and_scenes(request, context, errors, scene_errors):
    #make sure we have an uploaded scenelist file
    if not request.FILES.has_key("scenelist"):
        errors['file'] = "Please provide a scene list and include at least one scene for processing."
    else:
        #there was a file attached to the request.  make sure its not empty.
        orderfile = request.FILES['scenelist']
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
# Validation for the form request from users
####################################################################################################################
def validate_input_params(request):
        
    context, errors, scene_errors = {},{}, list()
        
    validate_email(request, context, errors)
    validate_files_and_scenes(request, context, errors, scene_errors)
    #validate_product_options(request, context, errors)        

    #Look for an order_description and put it into the context if available
    if request.POST.has_key('order_description'):
        context['order_description'] = request.POST['order_description']
        
    return (context, errors, scene_errors)