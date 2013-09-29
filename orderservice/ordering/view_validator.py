import lta

####################################################################################################################
# Validates all the projection params if the user chose to reproject
####################################################################################################################
def validate_projection_params(request, options, option_errors):
    #we only need to check this if the user chose to reproject
    if options['reproject'] == True:
        #make sure a projection was supplied and it was one of aea, lonlat, sinu, utm
        pass
            
####################################################################################################################
# Validates all the image extents for image resizing/reshaping
####################################################################################################################
def validate_boundingbox_params(request, options, option_errors):
    if options['image_extents'] == True:
        #make sure the other values were supplied
        #make sure the supplied values are numbers
        #make sure the supplied values are in range (for dd anyway)
        #
        pass
    
####################################################################################################################
# Ensures the supplied pixel_size and pixel_size_units are valid
####################################################################################################################
def validate_pixelsize_params(request, options, option_errors):
    if options['resize'] == True:
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
        if request.POST.has_key(o) and request.POST[o] == True:
            default_options[o] = True
           
    validate_projection_params(request, default_options, prod_option_errors)
    validate_boundingbox_params(request, default_options, prod_option_errors)
    validate_pixelsize_params(request, default_options, prod_option_errors)
        
    return (default_options,prod_option_errors)
        
    
####################################################################################################################
#Ensures an email was supplied
####################################################################################################################
def validate_email(context, errors):
        
    #start off by making sure we have an email address.
    if not request.POST.has_key('email') or not core.validate_email(request.POST['email']):    
        errors['email'] = "Please provide a valid email address"
    else:
        context['email'] = request.POST['email']
            
####################################################################################################################
#Checks for the prescence of a scenelist, checks it for errors.
####################################################################################################################
def validate_files_and_scenes(context, errors, scene_errors):
    #make sure we have an uploaded scenelist file
    if not request.FILES.has_key("file"):
        errors['file'] = "Please provide a scene list and include at least one scene for processing."
    else:
        #there was a file attached to the request.  make sure its not empty.
        orderfile = request.FILES['file']
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
        
    validate_email(context, errors)
    validate_files_and_scenes(context, errors, scene_errors)
    validate_product_options(context,errors)        

    #Look for an order_description and put it into the context if available
    if request.POST.has_key('order_description'):
        context['order_description'] = request.POST['order_description']
        
    return (context, errors, scene_errors)